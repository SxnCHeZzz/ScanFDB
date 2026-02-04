using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FirebirdSql.Data.FirebirdClient;
using Dapper;


// Класс для хранения информации о БД
public class ScanFdb
{
    public string FilePath { get; set; } = string.Empty;
    public bool IsReadable { get; set; }
    public string? DatabaseType { get; set; }
    public string? FarmName { get; set; }
    public string? Region { get; set; }
    public int? NHoz { get; set; }
    public int? NObl { get; set; } 
    public string? DatabaseCode { get; set; }
    public string? ErrorMessage { get; set; }

    // SELEX метрики
    public DateTime? LastCalvingDate { get; set; }
    public DateTime? LastMilkControlDate { get; set; }
    public string? BreedDistribution { get; set; }

    // BULLS метрики
    public DateTime? LastSemenEditDate { get; set; }
    public string? SemenDistribution { get; set; }
    public int? BullsAndSemenCount { get; set; }

    public override string ToString()
    {
        return IsReadable
            ? $"{FilePath}: {DatabaseType} - {FarmName} ({Region})"
            : $"{FilePath}: Ошибка - {ErrorMessage}";
    }
}

// Класс для данных из таблицы SHOZ
public class ShozData
{
    public string? IM { get; set; }
    public int NOBL { get; set; }
}

// Класс для распределения пород
public class BreedDistribution
{
    public int NPOR { get; set; }
    public int CNT { get; set; }
}

// Класс для распределения семени по годам
public class SemenDistribution
{
    public int GOD { get; set; }
    public decimal DZCNT { get; set; }
}



public class DatabaseScanner
{
    private SemaphoreSlim? _connectionSemaphore;
    private int _processedCount;
    private int _successCount;
    private int _errorCount;

 
    private static readonly Encoding Win1251 = Encoding.GetEncoding(1251);
    private static readonly Encoding Utf8 = Encoding.UTF8;

    
    public async Task<List<ScanFdb>> ScanDirectoryAsync(
        string directoryPath,
        int maxConcurrentConnections,
        CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(directoryPath))
        {
            throw new DirectoryNotFoundException($"Каталог '{directoryPath}' не существует.");
        }

        _connectionSemaphore = new SemaphoreSlim(maxConcurrentConnections, maxConcurrentConnections);
        _processedCount = 0;
        _successCount = 0;
        _errorCount = 0;

        var results = new List<ScanFdb>();

       
        var fdbFiles = Directory.GetFiles(directoryPath, "*.fdb", SearchOption.AllDirectories);
        Logger.LogInfo($"Найдено {fdbFiles.Length} файлов .fdb для обработки");

        
        var tasks = fdbFiles.Select(filePath =>
            ProcessDatabaseAsync(filePath, cancellationToken));

        var scanResults = await Task.WhenAll(tasks);
        results.AddRange(scanResults);

        Logger.LogInfo($"Сканирование завершено. Успешно: {_successCount}, Ошибок: {_errorCount}");

        return results;
    }

    // Обрабатывает отдельный файл базы данных
    private async Task<ScanFdb> ProcessDatabaseAsync(
    string filePath,
    CancellationToken cancellationToken)
    {
        var fileName = Path.GetFileName(filePath);
        bool acquired = false;
        var result = new ScanFdb { FilePath = filePath };

        if (IsPathTooLong(filePath))
        {
            result.IsReadable = false;
            result.ErrorMessage = "Слишком длинный путь к файлу";
            Interlocked.Increment(ref _errorCount);
            Logger.LogError($"Файл {fileName}: {result.ErrorMessage} ({filePath})");
            Console.WriteLine($"[ОШИБКА ДЛИННОГО ПУТИ] {filePath}: {result.ErrorMessage}");
            return result;
        }

        try
        {
            await _connectionSemaphore!.WaitAsync(cancellationToken);
            acquired = true;
            try
            {
                if (!CanOpenFile(filePath))
                {
                    result.IsReadable = false;
                    result.ErrorMessage = "Файл заблокирован или поврежден";
                    Interlocked.Increment(ref _errorCount);
                    Logger.LogWarning($"Файл {fileName} заблокирован или поврежден");
                    Console.WriteLine($"[ОШИБКА ДОСТУПА К ФАЙЛУ] {filePath}: {result.ErrorMessage}");
                    return result;
                }

                await ProcessSingleFileAsync(filePath, result, cancellationToken);
                Interlocked.Increment(ref _successCount);
                Logger.LogInfo($"Файл {fileName} успешно обработан: {result.DatabaseType} - {result.FarmName}");
            }
            catch (Exception ex)
            {
                // Ошибки чтения данных (после успешного подключения)
                result.IsReadable = false;
                result.ErrorMessage = $"Ошибка чтения данных из БД: {GetUserFriendlyErrorMessage(ex)}";
                Interlocked.Increment(ref _errorCount);
                Logger.LogError($"Ошибка чтения для файла {fileName}: {ex.Message}");
                Console.WriteLine($"[ОШИБКА ЧТЕНИЯ] {filePath}: {result.ErrorMessage}");
            }
            finally
            {
                Interlocked.Increment(ref _processedCount);
            }
            return result;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            result.IsReadable = false;
            result.ErrorMessage = "Ошибка доступа к семафору: " + ex.Message;
            Interlocked.Increment(ref _errorCount);
            Logger.LogError($"Критическая ошибка семафора для {fileName}: {ex.Message}");
            Console.WriteLine($"[КРИТИЧЕСКАЯ ОШИБКА] {filePath}: {result.ErrorMessage}");
            return result;
        }
        finally
        {
            if (acquired)
            {
                _connectionSemaphore.Release();
            }
        }
    }

    // Проверяет, превышает ли путь 255 байт в UTF-8
    private bool IsPathTooLong(string filePath)
    {
        return Encoding.UTF8.GetByteCount(filePath) > 255;
    }

    // Проверяет, можно ли открыть файл
    private bool CanOpenFile(string filePath)
    {
        try
        {
            using var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            return true;
        }
        catch
        {
            return false;
        }
    }

    // Получает сообщение об ошибке
    private string GetUserFriendlyErrorMessage(Exception ex)
    {
        if (ex is FbException fbEx)
        {
            if (fbEx.Message.Contains("I/O error"))
                return "Ошибка ввода-вывода (файл поврежден)";
            if (fbEx.Message.Contains("file is not a valid database"))
                return "Файл не является корректной БД Firebird";
            if (fbEx.Message.Contains("lock conflict"))
                return "БД заблокирована другим процессом";
            if (fbEx.Message.Contains("no permission for"))
                return "Нет прав доступа к файлу";
            if (fbEx.Message.Contains("cannot start thread"))
                return "Ошибка запуска потока Firebird";
        }

        return ex.Message.Length > 100 ? ex.Message.Substring(0, 100) + "..." : ex.Message;
    }

    private async Task ProcessSingleFileAsync(
     string fdbFile,
     ScanFdb result,
     CancellationToken cancellationToken)
    {
        var fileName = Path.GetFileName(fdbFile);

        
        var connectionStrings = new[]
        {
                //Без Charset + большой кэш 
                $"User=sysdba;Password=masterkey;DataSource=localhost;Port=3050;DefaultDbCachePages=10000;Database={fdbFile};Dialect=3;Connection timeout=30;Pooling=false",

                //  С UTF8
                $"User=sysdba;Password=masterkey;DataSource=localhost;Port=3050;DefaultDbCachePages=10000;Database={fdbFile};Dialect=3;Charset=UTF8;Connection timeout=30;Pooling=false",

                // С NONE
                $"User=sysdba;Password=masterkey;DataSource=localhost;Port=3050;DefaultDbCachePages=10000;Database={fdbFile};Dialect=3;Charset=NONE;Connection timeout=30;Pooling=false",

                // С WIN1251 
                $"User=sysdba;Password=masterkey;DataSource=localhost;Port=3050;DefaultDbCachePages=10000;Database={fdbFile};Dialect=3;Charset=WIN1251;Connection timeout=30;Pooling=false",

                // Остальные 
                $"User=sysdba;Password=masterkey;DataSource=localhost;Port=3050;Database={fdbFile};Dialect=1;Charset=NONE;Connection timeout=30;Pooling=false",
                $"User=sysdba;Password=masterkey;DataSource=localhost;Port=3050;Database={fdbFile};Dialect=1;Connection timeout=30;Pooling=false",
                $"User=SYSDBA;Password=masterkey;DataSource=localhost;Port=3050;Database={fdbFile};Dialect=3;Charset=UTF8;Connection timeout=30;Pooling=false",
                $"User=sysdba;Password=masterke;DataSource=localhost;Port=3050;Database={fdbFile};Dialect=3;Charset=UTF8;Connection timeout=30;Pooling=false",
                $"User=SYSDBA;Password=;DataSource=localhost;Port=3050;Database={fdbFile};Dialect=3;Charset=UTF8;Connection timeout=30;Pooling=false",
                       };

        Exception? lastException = null;
        string? usedConnectionString = null;

        foreach (var connectionString in connectionStrings)
        {
            try
            {
                Logger.LogInfo($"Пробуем подключиться к {fileName} с строкой: {GetConnectionStringForLog(connectionString)}");
                using var connection = new FbConnection(connectionString);

                var connectionTask = connection.OpenAsync(cancellationToken);
                if (await Task.WhenAny(connectionTask, Task.Delay(TimeSpan.FromSeconds(20), cancellationToken)) != connectionTask)
                {
                    throw new TimeoutException("Таймаут подключения (20 секунд)");
                }
                await connectionTask;

                Logger.LogInfo($"Подключение к {fileName} успешно установлено");
                usedConnectionString = connectionString;

                string usedCharset = GetCharsetFromConnectionString(connectionString);
                Logger.LogInfo($"Успешный Charset: {usedCharset}");

                result.DatabaseType = await DetermineDatabaseTypeAsync(connection, cancellationToken);
                await GetFarmDataAsync(connection, result, usedCharset, cancellationToken);

                result.IsReadable = true;
                return;
            }
            catch (FbException ex) when (ex.Message.Contains("cannot start thread"))
            {
                lastException = ex;
                Logger.LogWarning($"Ошибка потока Firebird для {fileName}: {ex.Message}");
                await Task.Delay(2000, cancellationToken);
                continue;
            }
            catch (FbException ex) when (ex.Message.Contains("I/O error") ||
                                         ex.Message.Contains("lock conflict") ||
                                         ex.Message.Contains("file is not a valid database") ||
                                         ex.Message.Contains("Invalid character set"))
            {
                lastException = ex;
                Logger.LogWarning($"Ошибка Firebird для {fileName}: {ex.Message}");
                continue;
            }
            catch (FbException ex)
            {
                lastException = ex;
                Logger.LogWarning($"Ошибка Firebird для {fileName}: {ex.Message}");
                continue;
            }
            catch (Exception ex)
            {
                lastException = ex;
                Logger.LogError($"Общая ошибка для {fileName}: {ex.Message}");
                continue;
            }
        }

        result.IsReadable = false;
        result.ErrorMessage = $"Ошибка подключения к БД: {GetShortErrorMessage(lastException)}";
        Logger.LogError($"Не удалось подключиться к {fileName}: {result.ErrorMessage}");
        Console.WriteLine($"[ОШИБКА ПОДКЛЮЧЕНИЯ] {fdbFile}: {result.ErrorMessage}");
    }

    
    private string GetConnectionStringForLog(string connectionString)
    {
        return connectionString.Replace("masterkey", "*****")
                              .Replace("masterke", "*****");
    }

  
    private string GetShortErrorMessage(Exception? ex)
    {
        if (ex == null) return "Неизвестная ошибка";

        var msg = ex.Message;
        if (msg.Length > 50)
            return msg.Substring(0, 50) + "...";
        return msg;
    }

    // Определяет тип базы данных (BULLS или SELEX)
    private async Task<string> DetermineDatabaseTypeAsync(
        FbConnection connection,
        CancellationToken cancellationToken)
    {
        const string checkTableQuery =
            "SELECT 1 FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'G_CONSTB'";

        try
        {
            var result = await connection.ExecuteScalarAsync(checkTableQuery);
            return result != null ? "BULLS" : "SELEX";
        }
        catch (Exception ex)
        {
            Logger.LogError($"Ошибка при определении типа БД: {ex.Message}");
            return "UNKNOWN";
        }
    }
    private async Task GetFarmDataAsync(
        FbConnection connection,
        ScanFdb result,
        string usedCharset,
        CancellationToken cancellationToken)
    {
        try
        {
            string tableName = result.DatabaseType == "BULLS" ? "G_CONSTB" : "G_CONST";
            bool tableExists = await CheckTableExistsAsync(connection, tableName, cancellationToken);
            if (!tableExists)
            {
                string altTableName = tableName == "G_CONSTB" ? "G_CONST" : "G_CONSTB";
                if (await CheckTableExistsAsync(connection, altTableName, cancellationToken))
                {
                    tableName = altTableName;
                    result.DatabaseType = tableName == "G_CONSTB" ? "BULLS" : "SELEX";
                    Logger.LogInfo($"Используем альтернативную таблицу: {tableName}");
                }
                else
                {
                    tableName = await FindConstantsTableAsync(connection, cancellationToken);
                    if (string.IsNullOrEmpty(tableName))
                    {
                        throw new Exception($"Таблицы G_CONSTB или G_CONST не найдены в БД");
                    }
                    Logger.LogInfo($"Найдена таблица констант: {tableName}");
                }
            }

            int? nhozCurrent = await GetNhozCurrentAsync(connection, tableName, cancellationToken);
            if (!nhozCurrent.HasValue)
            {
                nhozCurrent = await GetNhozAlternativeAsync(connection, tableName, cancellationToken);
                if (!nhozCurrent.HasValue)
                {
                    throw new Exception("Не удалось получить OWN_NHOZ из БД");
                }
            }

            result.NHoz = nhozCurrent.Value; 

            var shozData = await GetShozDataAsync(connection, nhozCurrent.Value, cancellationToken);
            if (shozData == null || string.IsNullOrEmpty(shozData.IM))
            {
                shozData = await GetShozAlternativeAsync(connection, nhozCurrent.Value, cancellationToken);
                if (shozData == null || string.IsNullOrEmpty(shozData.IM))
                {
                    throw new Exception($"Не найдено хозяйство с NHOZ = {nhozCurrent.Value}");
                }
            }

            // Сохраняем NObl 
            result.NObl = shozData.NOBL;

            // Правильная конвертация:  NONE фиксим, для остальных — берём как есть
            string farmNameRaw = shozData.IM?.Trim() ?? string.Empty;
            result.FarmName = usedCharset == "NONE" ? FixNoneCharset(farmNameRaw) : farmNameRaw;

            Logger.LogInfo($"Найдено хозяйство: {result.FarmName}");

            string? regionRaw = await GetRegionNameAsync(connection, shozData.NOBL, cancellationToken);
            result.Region = string.IsNullOrEmpty(regionRaw)
                ? null
                : (usedCharset == "NONE" ? FixNoneCharset(regionRaw.Trim()) : regionRaw.Trim());

            Logger.LogInfo($"Найден регион: {result.Region}");

            // Получаем дополнительные метрики в зависимости от типа БД
            if (result.DatabaseType == "SELEX")
            {
                await GetSelexMetricsAsync(connection, result, usedCharset, cancellationToken);

                
                GenerateDatabaseCode(result);
            }
            else if (result.DatabaseType == "BULLS")
            {
                await GetBullsMetricsAsync(connection, result, usedCharset, cancellationToken);

                // Для BULLS код не генерируем или генерируем по-другому
                result.DatabaseCode = $"{result.NObl}_{result.NHoz}_BULLS";
            }
        }
        catch (Exception ex)
        {
            Logger.LogError($"Ошибка при получении данных о хозяйстве: {ex.Message}");
            throw;
        }
    }

    // Генерирует код базы данных для SELEX: nobl_nhoz_{коды пород из Распределения пород}
    private void GenerateDatabaseCode(ScanFdb result)
    {
        if (result.DatabaseType != "SELEX" ||
            string.IsNullOrEmpty(result.BreedDistribution) ||
            !result.NObl.HasValue ||
            !result.NHoz.HasValue)
        {
            // Для BULLS или если нет данных
            result.DatabaseCode = result.DatabaseType == "BULLS"
                ? $"BULLS_{result.NHoz}"
                : $"{result.NHoz}_NO_DATA";
            return;
        }

        try
        {
            // Извлекаем коды пород из строки распределения
            var breedCodes = new List<string>();

            var breedPairs = result.BreedDistribution.Split('|', StringSplitOptions.RemoveEmptyEntries);

            foreach (var pair in breedPairs)
            {
                var parts = pair.Trim().Split('-', StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length >= 1)
                {
                    var code = parts[0].Trim();
                    if (!string.IsNullOrEmpty(code))
                    {
                        breedCodes.Add(code);
                    }
                }
            }

            if (breedCodes.Any())
            {
                // Формируем код: nobl_nhoz_{коды пород}
                result.DatabaseCode = $"{result.NObl}_{result.NHoz}_{string.Join("_", breedCodes)}";
            }
            else
            {
                result.DatabaseCode = $"{result.NObl}_{result.NHoz}_NO_BREED_CODES";
            }

            Logger.LogInfo($"Сгенерирован код базы данных: {result.DatabaseCode}");
        }
        catch (Exception ex)
        {
            Logger.LogWarning($"Ошибка при генерации кода базы данных: {ex.Message}");
            result.DatabaseCode = $"{result.NObl}_{result.NHoz}_ERROR";
        }
    }

    // Ищет таблицу с константами
    private async Task<string?> FindConstantsTableAsync(
        FbConnection connection,
        CancellationToken cancellationToken)
    {
        var possibleTables = new[] { "G_CONSTB", "G_CONST"};

        foreach (var table in possibleTables)
        {
            if (await CheckTableExistsAsync(connection, table, cancellationToken))
            {
                return table;
            }
        }

        return null;
    }

    // Получает метрики для SELEX
    private async Task GetSelexMetricsAsync(
        FbConnection connection,
        ScanFdb result,
        string usedCharset,
        CancellationToken cancellationToken)
    {
        try
        {
            // 1. Дата последнего отела
            if (await CheckTableExistsAsync(connection, "EV_OTEL_REZ", cancellationToken))
            {
                const string lastCalvingQuery =
                    "select first 1 date_event from EV_OTEL_REZ order by DATE_EVENT desc";

                var lastCalving = await connection.ExecuteScalarAsync<DateTime?>(lastCalvingQuery);
                result.LastCalvingDate = lastCalving;
                Logger.LogInfo($"Дата последнего отела: {lastCalving}");
            }

            // 2. Дата последнего контроля молока
            if (await CheckTableExistsAsync(connection, "EV_KONTROL", cancellationToken))
            {
                const string lastMilkControlQuery =
                    "select first 1 date_event from EV_KONTROL order by DATE_EVENT desc";

                var lastMilkControl = await connection.ExecuteScalarAsync<DateTime?>(lastMilkControlQuery);
                result.LastMilkControlDate = lastMilkControl;
                Logger.LogInfo($"Дата последнего контроля молока: {lastMilkControl}");
            }

            // 3. Распределение коров по породам (с сортировкой по убыванию)
            if (await CheckTableExistsAsync(connection, "REGISTER", cancellationToken))
            {
                const string breedDistributionQuery = @"
                SELECT npor, count(npor) as cnt
                FROM Register
                WHERE CAST(nanimal AS VARCHAR(100)) LIKE '4%'
                AND DATE_V is null
                GROUP BY npor
                ORDER BY cnt DESC"; 

                var breeds = await connection.QueryAsync<BreedDistribution>(breedDistributionQuery);

                if (breeds != null && breeds.Any())
                {
                    
                    var breedList = breeds.ToList();

                    
                    var distribution = breedList.Select(b => $"{b.NPOR} - {b.CNT}");
                    result.BreedDistribution = string.Join(" | ", distribution);

                    
                    Logger.LogInfo($"Распределение пород: {result.BreedDistribution}");
                    Logger.LogInfo($"Всего пород: {breedList.Count}, " +
                                  $"Всего голов: {breedList.Sum(b => b.CNT)}");
                }
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning($"Ошибка при получении SELEX метрик: {ex.Message}");
        }
    }

    // Получает метрики для BULLS
    private async Task GetBullsMetricsAsync(
        FbConnection connection,
        ScanFdb result,
        string usedCharset,
        CancellationToken cancellationToken)
    {
        try
        {
            // 1. Дата последнего редактирования запасов семени
            if (await CheckTableExistsAsync(connection, "PRODUCE", cancellationToken))
            {
                const string lastSemenEditQuery =
                    "select first 1 date_edit from PRODUCE order by date_edit desc";

                var lastSemenEdit = await connection.ExecuteScalarAsync<DateTime?>(lastSemenEditQuery);
                result.LastSemenEditDate = lastSemenEdit;
                Logger.LogInfo($"Дата последнего редактирования семени: {lastSemenEdit}");
            }

            // 2. Распределение запасов семени по годам
            if (await CheckTableExistsAsync(connection, "PRODUCE", cancellationToken))
            {
                const string semenDistributionQuery = @"
                select first 1 *
                from
                (
                    select god, sum(ostatok_doz) dzcnt 
                    from PRODUCE 
                    group by god 
                    order by god desc
                ) foo";

                var semenDist = await connection.QueryFirstOrDefaultAsync<SemenDistribution>(semenDistributionQuery);

                if (semenDist != null)
                {
                    result.SemenDistribution = $"{semenDist.GOD} - {semenDist.DZCNT}";
                    Logger.LogInfo($"Распределение семени: {result.SemenDistribution}");
                }
            }

            // 3. Число быков и семени
            if (await CheckTableExistsAsync(connection, "REGISTER", cancellationToken))
            {
                const string bullsCountQuery =
                    "select count(*) from register where nprizn in (1,2)";

                var bullsCount = await connection.ExecuteScalarAsync<int?>(bullsCountQuery);
                result.BullsAndSemenCount = bullsCount;
                Logger.LogInfo($"Число быков и семени: {bullsCount}");
            }
        }
        catch (Exception ex)
        {
            Logger.LogWarning($"Ошибка при получении BULLS метрик: {ex.Message}");
        }
    }


    private async Task<int?> GetNhozAlternativeAsync(
        FbConnection connection,
        string tableName,
        CancellationToken cancellationToken)
    {
        var possibleNames = new[] { "OWN_NHOZ", "CURRENT_HOZ", "NHOZ", "HOZ_ID", "FARM_ID" };

        foreach (var name in possibleNames)
        {
            try
            {
                var sql = $"SELECT val FROM {tableName} WHERE name = '{name}'";
                var result = await connection.ExecuteScalarAsync(sql);

                if (result != null && result != DBNull.Value)
                {
                    string strValue = result.ToString()!;
                    strValue = strValue.Trim('\'', '"', ' ', '\t');

                    if (int.TryParse(strValue, out int nhoz))
                    {
                        Logger.LogInfo($"Найдено поле {name} со значением {nhoz}");
                        return nhoz;
                    }
                }
            }
            catch
            {
                continue;
            }
        }

        return null;
    }


    private async Task<ShozData?> GetShozAlternativeAsync(
        FbConnection connection,
        int nhoz,
        CancellationToken cancellationToken)
    {
        var possibleQueries = new[]
        {
            "SELECT im, nobl FROM shoz WHERE nhoz = @nhoz",
            "SELECT name as im, region_id as nobl FROM farms WHERE id = @nhoz",
            "SELECT farm_name as im, region_code as nobl FROM organizations WHERE farm_id = @nhoz"
        };

        foreach (var query in possibleQueries)
        {
            try
            {
                var result = await connection.QueryFirstOrDefaultAsync<ShozData>(query, new { nhoz });
                if (result != null && !string.IsNullOrEmpty(result.IM))
                {
                    Logger.LogInfo($"Найдены данные в альтернативной таблице");
                    return result;
                }
            }
            catch
            {
                continue;
            }
        }

        return null;
    }

    // Определяет, какой Charset был использован в успешной строке подключения
    private string GetCharsetFromConnectionString(string connectionString)
    {
        var parts = connectionString.Split(';');
        foreach (var part in parts)
        {
            var trimmed = part.Trim();
            if (trimmed.StartsWith("Charset=", StringComparison.OrdinalIgnoreCase))
            {
                return trimmed.Split('=')[1].Trim().ToUpperInvariant();
            }
        }
        return "NONE"; 
    }

   
    private string FixNoneCharset(string? input)
    {
        if (string.IsNullOrEmpty(input)) return input ?? string.Empty;

        try
        {
           
            byte[] latinBytes = Encoding.GetEncoding("ISO-8859-1").GetBytes(input);
            return Encoding.GetEncoding(1251).GetString(latinBytes);
        }
        catch
        {
            
            return input;
        }
    }

    // Конвертирует из WIN1251 в UTF8
    private string ConvertFromWin1251(string? input)
    {
        if (string.IsNullOrEmpty(input))
            return input ?? string.Empty;

        try
        {
            
            if (IsValidUtf8(input))
                return input;

          
            byte[] win1251Bytes = Win1251.GetBytes(input);
            return Utf8.GetString(win1251Bytes);
        }
        catch
        {
            return input;
        }
    }

    //Валидность строки в UTF8
    private bool IsValidUtf8(string text)
    {
        try
        {
            byte[] bytes = Utf8.GetBytes(text);
            string roundtrip = Utf8.GetString(bytes);
            return string.Equals(text, roundtrip, StringComparison.Ordinal);
        }
        catch
        {
            return false;
        }
    }

    // Проверяет существование таблицы в базе данных
    private async Task<bool> CheckTableExistsAsync(
        FbConnection connection,
        string tableName,
        CancellationToken cancellationToken)
    {
        const string query =
            "SELECT 1 FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = @tableName";

        try
        {
            var result = await connection.ExecuteScalarAsync(query, new { tableName = tableName.ToUpper() });
            return result != null;
        }
        catch (Exception ex)
        {
            Logger.LogWarning($"Ошибка при проверке таблицы {tableName}: {ex.Message}");
            return false;
        }
    }

    // Получает значение OWN_NHOZ из таблицы констант
    private async Task<int?> GetNhozCurrentAsync(
        FbConnection connection,
        string tableName,
        CancellationToken cancellationToken)
    {
        const string query = "SELECT val FROM {0} WHERE name = 'OWN_NHOZ'";

        try
        {
            var sql = string.Format(query, tableName);
            var result = await connection.ExecuteScalarAsync(sql);

            if (result != null && result != DBNull.Value)
            {
                string strValue = result.ToString()!;
                strValue = strValue.Trim('\'', '"', ' ', '\t');

                if (int.TryParse(strValue, out int nhoz))
                {
                    return nhoz;
                }
            }

            return null;
        }
        catch (Exception ex)
        {
            Logger.LogWarning($"Ошибка при получении OWN_NHOZ: {ex.Message}");
            return null;
        }
    }

    // Получает данные из таблицы SHOZ для указанного NHOZ
    private async Task<ShozData?> GetShozDataAsync(
        FbConnection connection,
        int nhoz,
        CancellationToken cancellationToken)
    {
        const string query = "SELECT im, nobl FROM shoz WHERE nhoz = @nhoz";

        try
        {
            var result = await connection.QueryFirstOrDefaultAsync<ShozData>(query, new { nhoz });
            return result;
        }
        catch (Exception ex)
        {
            Logger.LogWarning($"Ошибка при получении SHOZ данных: {ex.Message}");
            return null;
        }
    }

    // Получает название региона из таблицы SOBL
    private async Task<string?> GetRegionNameAsync(
        FbConnection connection,
        int nobl,
        CancellationToken cancellationToken)
    {
        const string query = "SELECT im FROM sobl WHERE nobl = @nobl";

        try
        {
            var result = await connection.ExecuteScalarAsync<string>(query, new { nobl });
            return result;
        }
        catch (Exception ex)
        {
            Logger.LogWarning($"Ошибка при получении региона: {ex.Message}");
            return null;
        }
    }
}

// Логгер 
public static class Logger
{
    private static readonly object _lock = new();
    private static string? _logFilePath;

    public static void Initialize(string logFilePath)
    {
        _logFilePath = logFilePath;

        lock (_lock)
        {
            File.WriteAllText(_logFilePath,
                $"Лог сканирования Firebird БД\n" +
                $"Начало: {DateTime.Now:yyyy-MM-dd HH:mm:ss}\n" +
                $"{"=".PadRight(80, '=')}\n",
                Encoding.UTF8);
        }
    }

    public static void LogInfo(string message)
    {
        Log("INFO", message);
    }

    public static void LogWarning(string message)
    {
        Log("WARN", message);
    }

    public static void LogError(string message)
    {
        Log("ERROR", message);
    }

    public static void LogError(Exception exception)
    {
        Log("ERROR", $"{exception.Message}\n{exception.StackTrace}");
    }

    private static void Log(string level, string message)
    {
        if (_logFilePath == null)
            return;

        var logMessage = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss} [{level}] {message}";

        lock (_lock)
        {
            try
            {
                File.AppendAllText(_logFilePath, logMessage + Environment.NewLine, Encoding.UTF8);
            }
            catch
            {

            }
        }
    }
}
// Экспорт в CSV
public static class CsvExporter
{
    public static async Task ExportToCsvAsync(List<ScanFdb> results, string outputPath)
    {
        try
        {
            using var writer = new StreamWriter(outputPath, false, Encoding.UTF8);

            await writer.WriteLineAsync(
                "Полный путь;" +
                "Читаемость;" +
                "Nhoz;" +
                "Код базы данных;" +
                "Название хозяйства;" +
                "Регион;" +
                "Тип БД;" +
                "Дата последнего отела;" +
                "Дата последнего контроля молока;" +
                "Распределение пород;" +
                "Дата последнего редактирования семени;" +
                "Распределение семени;" +
                "Число быков и семени;" +
                "Сообщение об ошибке");

            foreach (var result in results.OrderBy(r => r.FilePath))
            {
                var fullPath = result.FilePath;
                var readability = result.IsReadable ? "Успешно" : "Ошибка";
                var nhoz = result.NHoz?.ToString() ?? "Н/Д";
                var databaseCode = result.DatabaseCode ?? "Н/Д"; 
                var farmName = result.FarmName ?? "Н/Д";
                var region = result.Region ?? "Н/Д";
                var dbType = result.DatabaseType ?? "Н/Д";

                // SELEX метрики
                var lastCalvingDate = result.LastCalvingDate?.ToString("yyyy-MM-dd") ?? "Н/Д";
                var lastMilkControlDate = result.LastMilkControlDate?.ToString("yyyy-MM-dd") ?? "Н/Д";
                var breedDistribution = result.BreedDistribution ?? "Н/Д";

                // BULLS метрики
                var lastSemenEditDate = result.LastSemenEditDate?.ToString("yyyy-MM-dd") ?? "Н/Д";
                var semenDistribution = result.SemenDistribution ?? "Н/Д";
                var bullsCount = result.BullsAndSemenCount?.ToString() ?? "Н/Д";

                // Сообщение об ошибке 
                var errorMessage = result.IsReadable ? string.Empty : (result.ErrorMessage ?? "Неизвестная ошибка");

                await writer.WriteLineAsync(
                    $"{EscapeCsv(fullPath)};" +
                    $"{EscapeCsv(readability)};" +
                    $"{EscapeCsv(nhoz)};" +
                    $"{EscapeCsv(databaseCode)};" +
                    $"{EscapeCsv(farmName)};" +
                    $"{EscapeCsv(region)};" +
                    $"{EscapeCsv(dbType)};" +
                    $"{EscapeCsv(lastCalvingDate)};" +
                    $"{EscapeCsv(lastMilkControlDate)};" +
                    $"{EscapeCsv(breedDistribution)};" +
                    $"{EscapeCsv(lastSemenEditDate)};" +
                    $"{EscapeCsv(semenDistribution)};" +
                    $"{EscapeCsv(bullsCount)};" +
                    $"{EscapeCsv(errorMessage)}");
            }

            Logger.LogInfo($"CSV файл сохранен: {outputPath}");

            // Анализ дубликатов
            await AnalyzeDuplicatesAsync(results, outputPath);
        }
        catch (Exception ex)
        {
            Logger.LogError($"Ошибка при сохранении CSV: {ex.Message}");
            throw new Exception($"Ошибка при сохранении CSV: {ex.Message}", ex);
        }
    }


    // Анализ дубликатов
    private static async Task AnalyzeDuplicatesAsync(List<ScanFdb> results, string outputPath)
    {
        try
        {
            var readableResults = results.Where(r => r.IsReadable && r.NHoz.HasValue).ToList();
            var nhozGroups = readableResults
                .GroupBy(r => r.NHoz!.Value)
                .Where(g => g.Count() > 1)
                .ToList();

            if (!nhozGroups.Any())
            {
                Logger.LogInfo("Дубликаты NHoz не найдены");
                return;
            }

            
            var duplicatesPath = Path.Combine(
                Path.GetDirectoryName(outputPath) ?? Directory.GetCurrentDirectory(),
                $"duplicates_analysis_{DateTime.Now:yyyyMMdd_HHmmss}.log");

            var copiesLogPath = Path.Combine(
                Path.GetDirectoryName(outputPath) ?? Directory.GetCurrentDirectory(),
                $"exact_copies_{DateTime.Now:yyyyMMdd_HHmmss}.log");

            var versionsLogPath = Path.Combine(
                Path.GetDirectoryName(outputPath) ?? Directory.GetCurrentDirectory(),
                $"different_versions_{DateTime.Now:yyyyMMdd_HHmmss}.log");

            using var mainWriter = new StreamWriter(duplicatesPath, false, Encoding.UTF8);
            using var copiesWriter = new StreamWriter(copiesLogPath, false, Encoding.UTF8);
            using var versionsWriter = new StreamWriter(versionsLogPath, false, Encoding.UTF8);

            
            await mainWriter.WriteLineAsync($"Анализ дубликатов NHoz");
            await mainWriter.WriteLineAsync($"Всего дублированных NHoz: {nhozGroups.Count}");
            await mainWriter.WriteLineAsync($"Время анализа: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            await mainWriter.WriteLineAsync($"{"=".PadRight(80, '=')}\n");

            await copiesWriter.WriteLineAsync($"ТОЧНЫЕ КОПИИ ФАЙЛОВ (одинаковые метрики)");
            await copiesWriter.WriteLineAsync($"Время анализа: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            await copiesWriter.WriteLineAsync($"{"=".PadRight(80, '=')}\n");

            await versionsWriter.WriteLineAsync($"РАЗНЫЕ ВЕРСИИ ХОЗЯЙСТВ (разные метрики)");
            await versionsWriter.WriteLineAsync($"Время анализа: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            await versionsWriter.WriteLineAsync($"{"=".PadRight(80, '=')}\n");

            int totalCopies = 0;
            int totalVersions = 0;

            foreach (var group in nhozGroups)
            {
                await mainWriter.WriteLineAsync($"\nАнализ NHoz: {group.Key}");
                await mainWriter.WriteLineAsync($"Всего файлов с этим NHoz: {group.Count()}");

                var files = group.ToList();

                
                var selexFiles = files.Where(f => f.DatabaseType == "SELEX").ToList();
                var bullsFiles = files.Where(f => f.DatabaseType == "BULLS").ToList();

                await mainWriter.WriteLineAsync($"  SELEX файлов: {selexFiles.Count}");
                await mainWriter.WriteLineAsync($"  BULLS файлов: {bullsFiles.Count}");

                // Проверяем копии в рамках каждого типа БД
                var copiesVersionsResult1 = await CheckCopiesAndVersionsAsync(
                    selexFiles,
                    group.Key,
                    mainWriter,
                    copiesWriter,
                    versionsWriter);
                totalCopies += copiesVersionsResult1.copiesCount;
                totalVersions += copiesVersionsResult1.versionsCount;

                var copiesVersionsResult2 = await CheckCopiesAndVersionsAsync(
                    bullsFiles,
                    group.Key,
                    mainWriter,
                    copiesWriter,
                    versionsWriter);
                totalCopies += copiesVersionsResult2.copiesCount;
                totalVersions += copiesVersionsResult2.versionsCount;
            }

            // Итоги
            await mainWriter.WriteLineAsync($"\n{"=".PadRight(80, '=')}");
            await mainWriter.WriteLineAsync($"ИТОГИ:");
            await mainWriter.WriteLineAsync($"Всего групп дубликатов: {nhozGroups.Count}");
            await mainWriter.WriteLineAsync($"Точных копий файлов: {totalCopies} групп");
            await mainWriter.WriteLineAsync($"Разных версий хозяйств: {totalVersions} групп");

            await copiesWriter.WriteLineAsync($"\nИТОГО: Найдено {totalCopies} групп точных копий");
            await versionsWriter.WriteLineAsync($"\nИТОГО: Найдено {totalVersions} групп разных версий");

            Logger.LogInfo($"Анализ дубликатов сохранен: {duplicatesPath}");
            Logger.LogInfo($"Лог точных копий: {copiesLogPath}");
            Logger.LogInfo($"Лог разных версий: {versionsLogPath}");
        }
        catch (Exception ex)
        {
            Logger.LogError($"Ошибка при анализе дубликатов: {ex.Message}");
        }
    }

    // Метод проверки копий и версий 
    private static async Task<(int copiesCount, int versionsCount)> CheckCopiesAndVersionsAsync(
        List<ScanFdb> files,
        int nhoz,
        StreamWriter mainWriter,
        StreamWriter copiesWriter,
        StreamWriter versionsWriter)
    {
        int copiesCount = 0;
        int versionsCount = 0;

        if (files.Count < 2) return (copiesCount, versionsCount);

        // Сравниваем все файлы между собой
        var comparisonResults = new List<(ScanFdb file1, ScanFdb file2, bool areEqual, string? reason)>();

        for (int i = 0; i < files.Count; i++)
        {
            for (int j = i + 1; j < files.Count; j++)
            {
                bool areEqual = AreMetricsEqual(files[i], files[j], out string? reason);
                comparisonResults.Add((files[i], files[j], areEqual, reason));
            }
        }

        // Определяем, есть ли полные копии
        bool allMetricsMatch = comparisonResults.All(r => r.areEqual);

        await mainWriter.WriteLineAsync($"  Тип БД: {files[0].DatabaseType}");

        if (allMetricsMatch)
        {
            // ВСЕ файлы имеют одинаковые метрики - это копии
            await mainWriter.WriteLineAsync($"  -> ВНИМАНИЕ: Все файлы имеют одинаковые метрики - вероятно это копии одного файла");

            await copiesWriter.WriteLineAsync($"\nNHoz: {nhoz} (Тип БД: {files[0].DatabaseType})");
            await copiesWriter.WriteLineAsync($"Файлов: {files.Count}");
            await copiesWriter.WriteLineAsync($"Хозяйство: {files[0].FarmName ?? "Н/Д"}");

            foreach (var file in files)
            {
                await copiesWriter.WriteLineAsync($"  • {file.FilePath}");
                try
                {
                    var fileInfo = new FileInfo(file.FilePath);
                    if (fileInfo.Exists)
                    {
                        await copiesWriter.WriteLineAsync($"    Размер: {fileInfo.Length:N0} байт");
                        await copiesWriter.WriteLineAsync($"    Дата изменения: {fileInfo.LastWriteTime:yyyy-MM-dd HH:mm:ss}");
                    }
                }
                catch
                {
                    await copiesWriter.WriteLineAsync($"    [Информация о файле недоступна]");
                }
            }

            // Выводим метрики
            if (files[0].DatabaseType == "SELEX")
            {
                await copiesWriter.WriteLineAsync($"    Метрики SELEX:");
                await copiesWriter.WriteLineAsync($"      Последний отел: {files[0].LastCalvingDate?.ToString("yyyy-MM-dd") ?? "Н/Д"}");
                await copiesWriter.WriteLineAsync($"      Последний контроль: {files[0].LastMilkControlDate?.ToString("yyyy-MM-dd") ?? "Н/Д"}");
                await copiesWriter.WriteLineAsync($"      Распределение пород: {files[0].BreedDistribution ?? "Н/Д"}");
            }
            else if (files[0].DatabaseType == "BULLS")
            {
                await copiesWriter.WriteLineAsync($"    Метрики BULLS:");
                await copiesWriter.WriteLineAsync($"      Последнее редакт. семени: {files[0].LastSemenEditDate?.ToString("yyyy-MM-dd") ?? "Н/Д"}");
                await copiesWriter.WriteLineAsync($"      Распределение семени: {files[0].SemenDistribution ?? "Н/Д"}");
                await copiesWriter.WriteLineAsync($"      Быков и семени: {files[0].BullsAndSemenCount?.ToString() ?? "Н/Д"}");
            }

            copiesCount = 1;
        }
        else
        {
            // Есть различия в метриках - разные версии
            await mainWriter.WriteLineAsync($"  -> ВНИМАНИЕ: Файлы имеют разные метрики - это разные версии одного хозяйства");

            await versionsWriter.WriteLineAsync($"\nNHoz: {nhoz} (Тип БД: {files[0].DatabaseType})");
            await versionsWriter.WriteLineAsync($"Файлов: {files.Count}");
            await versionsWriter.WriteLineAsync($"Хозяйство: {files[0].FarmName ?? "Н/Д"}");

            // Показываем различия
            var mismatches = comparisonResults.Where(r => !r.areEqual).Take(3); // Берем первые 3 различия
            foreach (var mismatch in mismatches)
            {
                await versionsWriter.WriteLineAsync($"  Различие: {mismatch.reason ?? "Неизвестное различие"}");
            }

            // Выводим все файлы с их метриками
            await versionsWriter.WriteLineAsync($"  Файлы:");
            foreach (var file in files)
            {
                await versionsWriter.WriteLineAsync($"    • {file.FilePath}");

                if (file.DatabaseType == "SELEX")
                {
                    await versionsWriter.WriteLineAsync($"      Последний отел: {file.LastCalvingDate?.ToString("yyyy-MM-dd") ?? "Н/Д"}");
                    await versionsWriter.WriteLineAsync($"      Последний контроль: {file.LastMilkControlDate?.ToString("yyyy-MM-dd") ?? "Н/Д"}");
                    await versionsWriter.WriteLineAsync($"      Распределение пород: {file.BreedDistribution ?? "Н/Д"}");
                }
                else if (file.DatabaseType == "BULLS")
                {
                    await versionsWriter.WriteLineAsync($"      Последнее редакт. семени: {file.LastSemenEditDate?.ToString("yyyy-MM-dd") ?? "Н/Д"}");
                    await versionsWriter.WriteLineAsync($"      Распределение семени: {file.SemenDistribution ?? "Н/Д"}");
                    await versionsWriter.WriteLineAsync($"      Быков и семени: {file.BullsAndSemenCount?.ToString() ?? "Н/Д"}");
                }
            }

            versionsCount = 1;
        }

        return (copiesCount, versionsCount);
    }

    // Сравнение метрик
    private static bool AreMetricsEqual(ScanFdb file1, ScanFdb file2, out string? reason)
    {
        reason = null;

        if (file1.DatabaseType != file2.DatabaseType)
        {
            reason = $"Разные типы БД: {file1.DatabaseType} vs {file2.DatabaseType}";
            return false;
        }

        if (file1.DatabaseType == "SELEX")
        {
            // Сравниваем метрики SELEX 
            // 1. Дата последнего отела
            if (!AreDatesEqual(file1.LastCalvingDate, file2.LastCalvingDate))
            {
                reason = $"Разные даты последнего отела: " +
                         $"{file1.LastCalvingDate?.ToString("yyyy-MM-dd") ?? "null"} vs " +
                         $"{file2.LastCalvingDate?.ToString("yyyy-MM-dd") ?? "null"}";
                return false;
            }

            // 2. Дата последнего контроля молока
            if (!AreDatesEqual(file1.LastMilkControlDate, file2.LastMilkControlDate))
            {
                reason = $"Разные даты последнего контроля молока: " +
                         $"{file1.LastMilkControlDate?.ToString("yyyy-MM-dd") ?? "null"} vs " +
                         $"{file2.LastMilkControlDate?.ToString("yyyy-MM-dd") ?? "null"}";
                return false;
            }

            // 3. Распределение пород
            if (file1.BreedDistribution != file2.BreedDistribution)
            {
                reason = "Разное распределение пород";
                return false;
            }
        }
        else if (file1.DatabaseType == "BULLS")
        {
            // Сравниваем метрики BULLS
            // 1. Дата последнего редактирования запасов семени
            if (!AreDatesEqual(file1.LastSemenEditDate, file2.LastSemenEditDate))
            {
                reason = $"Разные даты последнего редактирования семени: " +
                         $"{file1.LastSemenEditDate?.ToString("yyyy-MM-dd") ?? "null"} vs " +
                         $"{file2.LastSemenEditDate?.ToString("yyyy-MM-dd") ?? "null"}";
                return false;
            }

            // 2. Распределение запасов семени по годам
            if (file1.SemenDistribution != file2.SemenDistribution)
            {
                reason = "Разное распределение семени";
                return false;
            }

            // 3. Число быков и семени
            if (file1.BullsAndSemenCount != file2.BullsAndSemenCount)
            {
                reason = $"Разное количество быков и семени: " +
                         $"{file1.BullsAndSemenCount} vs {file2.BullsAndSemenCount}";
                return false;
            }
        }

        return true;
    }

    // Вспомогательный метод для сравнения дат
    private static bool AreDatesEqual(DateTime? date1, DateTime? date2)
    {
        if (date1.HasValue && date2.HasValue)
        {
            return date1.Value.Date == date2.Value.Date;
        }
        return !date1.HasValue && !date2.HasValue;
    }

    private static string EscapeCsv(string value)
    {
        if (string.IsNullOrEmpty(value))
            return string.Empty;
        if (value.Contains(';') || value.Contains('"') || value.Contains('\n') || value.Contains('\r'))
            return $"\"{value.Replace("\"", "\"\"")}\"";
        return value;
    }
}