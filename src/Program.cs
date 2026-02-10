using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

public class Program
{
    static async Task Main(string[] args)
    {
        Console.OutputEncoding = System.Text.Encoding.UTF8;
        Console.Clear();
        PrintHeader();

        await RunInteractive();
    }

    static void PrintHeader()
    {
        Console.WriteLine("------------------------------------------------");
        Console.WriteLine("         Скан БД Firebird (SELEX/BULLS)         ");
        Console.WriteLine("------------------------------------------------");
        Console.WriteLine();
    }

    static void PrintSection(string title)
    {
        Console.WriteLine();
        Console.WriteLine($"{title}");
        Console.WriteLine(new string('-', title.Length));
        Console.WriteLine();
    }

    static void PrintInfo(string message)
    {
        Console.WriteLine($"  {message}");
    }

    static void PrintSuccess(string message)
    {
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"  {message}");
        Console.ResetColor();
    }

    static void PrintWarning(string message)
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine($"  {message}");
        Console.ResetColor();
    }

    static void PrintError(string message)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"  {message}");
        Console.ResetColor();
    }

    static async Task RunInteractive()
    {
        PrintSection("НАСТРОЙКА ПУТЕЙ");

        Console.WriteLine("Введите путь к каталогу для сканирования:");
        Console.WriteLine("(например: C:\\Databases или оставьте пустым для текущей папки)");
        Console.Write("> ");

        string? scanPath = Console.ReadLine()?.Trim();

        if (string.IsNullOrEmpty(scanPath))
        {
            scanPath = Directory.GetCurrentDirectory();
            PrintInfo($"Используется текущая папка: {scanPath}");
        }
        else if (!Directory.Exists(scanPath))
        {
            PrintError($"Каталог '{scanPath}' не существует.");
            Console.WriteLine();
            Console.WriteLine("Нажмите любую клавишу для выхода...");
            Console.ReadKey();
            return;
        }

        // Создаем папку для результатов в сканируемой папке
        string resultsFolder = Path.Combine(scanPath, $"scan_results_{DateTime.Now:yyyyMMdd_HHmmss}");

        try
        {
            Directory.CreateDirectory(resultsFolder);
            PrintSuccess($"Создана папка для результатов: {Path.GetFileName(resultsFolder)}");
        }
        catch (Exception ex)
        {
            PrintWarning($"Не удалось создать папку результатов: {ex.Message}");
            resultsFolder = scanPath;
            PrintInfo($"Результаты будут сохранены в: {scanPath}");
        }

        // Пути по умолчанию
        string defaultCsvPath = Path.Combine(resultsFolder, "результаты_сканирования.csv");
        string defaultLogPath = Path.Combine(resultsFolder, "errors.log");

        Console.WriteLine();
        Console.WriteLine("Пути для сохранения результатов:");

        Console.WriteLine();
        Console.WriteLine("CSV файл с результатами:");
        Console.WriteLine($"(по умолчанию: {Path.GetFileName(resultsFolder)}\\результаты_сканирования.csv)");
        Console.Write("> ");
        string? csvPathInput = Console.ReadLine()?.Trim();
        string csvPath;

        if (string.IsNullOrEmpty(csvPathInput))
        {
            csvPath = defaultCsvPath;
        }
        else if (!Path.IsPathRooted(csvPathInput))
        {
            csvPath = Path.Combine(resultsFolder, csvPathInput);
        }
        else
        {
            csvPath = csvPathInput;
        }

        Console.WriteLine();
        Console.WriteLine("Файл логов (ошибки и предупреждения):");
        Console.WriteLine($"(по умолчанию: {Path.GetFileName(resultsFolder)}\\errors.log)");
        Console.Write("> ");
        string? logPathInput = Console.ReadLine()?.Trim();
        string logPath;

        if (string.IsNullOrEmpty(logPathInput))
        {
            logPath = defaultLogPath;
        }
        else if (!Path.IsPathRooted(logPathInput))
        {
            logPath = Path.Combine(resultsFolder, logPathInput);
        }
        else
        {
            logPath = logPathInput;
        }

        int maxConnections = 5;

        PrintSection("ПОДТВЕРЖДЕНИЕ НАСТРОЕК");

        Console.WriteLine("Настройки сканирования:");
        Console.WriteLine();
        Console.WriteLine($"  Каталог для сканирования:");
        Console.WriteLine($"    {scanPath}");
        Console.WriteLine();
        Console.WriteLine($"  Файлы результатов:");
        Console.WriteLine($"    CSV: {csvPath}");
        Console.WriteLine($"    Логи: {logPath}");
        Console.WriteLine();

        Console.WriteLine("Начать сканирование? (y/n)");
        Console.Write("> ");
        string? confirm = Console.ReadLine()?.Trim().ToLower();

        if (confirm != "y" && confirm != "yes" && confirm != "д" && confirm != "да")
        {
            PrintInfo("Сканирование отменено.");
            Console.WriteLine();
            Console.WriteLine("Нажмите любую клавишу для выхода...");
            Console.ReadKey();
            return;
        }

        await RunScanner(scanPath, csvPath, logPath, maxConnections, resultsFolder);
    }

    static async Task RunScanner(string path, string output, string log, int maxConcurrent, string resultsFolder)
    {
        Console.Clear();
        PrintHeader();

        PrintSection("НАЧАЛО СКАНИРОВАНИЯ");

        PrintInfo($"Каталог: {path}");
        PrintInfo($"CSV: {Path.GetFileName(output)}");
        PrintInfo($"Логи: {Path.GetFileName(log)}");
        PrintInfo($"Папка результатов: {Path.GetFileName(resultsFolder)}");

        Console.WriteLine();
        PrintWarning("Нажмите ESC для отмены сканирования");
        Console.WriteLine();

        Logger.Initialize(log);

        try
        {
            var scanner = new DatabaseScanner();
            var cts = new CancellationTokenSource();

            // Отслеживания нажатия ESC
            var cancellationTask = Task.Run(async () =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    if (Console.KeyAvailable)
                    {
                        var key = Console.ReadKey(true);
                        if (key.Key == ConsoleKey.Escape)
                        {
                            Console.WriteLine();
                            PrintWarning("Отмена сканирования...");
                            cts.Cancel();
                            break;
                        }
                    }
                    await Task.Delay(100, cts.Token);
                }
            });

            // Оценка общего количества файлов
            var fdbFiles = Directory.GetFiles(path, "*.fdb", SearchOption.AllDirectories);
            Console.WriteLine($"Найдено файлов для обработки: {fdbFiles.Length}");
            Console.WriteLine();

            // Запускаем сканирование
            var results = await scanner.ScanDirectoryAsync(path, maxConcurrent, cts.Token);

            if (!cts.Token.IsCancellationRequested)
            {
                PrintSuccess("Сканирование завершено!");
                Console.WriteLine();

                // Сохраняем CSV
                PrintInfo("Сохранение результатов в CSV...");
                await CsvExporter.ExportToCsvAsync(results, output);
                PrintSuccess($"CSV сохранен: {Path.GetFileName(output)}");

                // Статистика
                var successful = results.Count(r => r.IsReadable);
                var selexCount = results.Count(r => r.DatabaseType == "SELEX" && r.IsReadable);
                var bullsCount = results.Count(r => r.DatabaseType == "BULLS" && r.IsReadable);
                var unknownCount = results.Count(r => r.IsReadable && r.DatabaseType != "SELEX" && r.DatabaseType != "BULLS");

                PrintSection("РЕЗУЛЬТАТЫ СКАНИРОВАНИЯ");

                Console.WriteLine("Статистика:");
                Console.WriteLine();
                Console.WriteLine($"  Всего файлов .fdb: {results.Count}");
                Console.WriteLine();
                Console.WriteLine($"  Успешно прочитано: {successful}");
                if (selexCount > 0) Console.WriteLine($"    • SELEX: {selexCount}");
                if (bullsCount > 0) Console.WriteLine($"    • BULLS: {bullsCount}");
                if (unknownCount > 0) Console.WriteLine($"    • Другие: {unknownCount}");
                Console.WriteLine();
                Console.WriteLine($"  Ошибок чтения: {results.Count - successful}");

                // Анализ уникальных NHoz
                var uniqueNHoz = results
                    .Where(r => r.IsReadable && r.NHoz.HasValue)
                    .Select(r => r.NHoz!.Value)
                    .Distinct()
                    .Count();

                Console.WriteLine();
                Console.WriteLine($"  Уникальных хозяйств (NHoz): {uniqueNHoz}");

                // Проверка дубликатов
                var duplicates = results
                    .Where(r => r.IsReadable && r.NHoz.HasValue)
                    .GroupBy(r => r.NHoz!.Value)
                    .Where(g => g.Count() > 1)
                    .ToList();

                if (duplicates.Any())
                {
                    Console.WriteLine();
                    PrintWarning($"Обнаружены дубликаты NHoz: {duplicates.Count}");

                    // Ищем логи анализа дубликатов
                    var logFiles = Directory.GetFiles(resultsFolder, "*duplicates*.*")
                        .Concat(Directory.GetFiles(resultsFolder, "*copies*.*"))
                        .Concat(Directory.GetFiles(resultsFolder, "*versions*.*"))
                        .Distinct()
                        .ToList();

                    if (logFiles.Any())
                    {
                        Console.WriteLine("  Файлы анализа дубликатов:");
                        foreach (var logFile in logFiles)
                        {
                            Console.WriteLine($"    • {Path.GetFileName(logFile)}");
                        }
                    }
                }

                // Показываем созданные файлы
                PrintSection("СОЗДАННЫЕ ФАЙЛЫ");

                try
                {
                    var createdFiles = Directory.GetFiles(resultsFolder)
                        .Select(f => new FileInfo(f))
                        .OrderBy(f => f.CreationTime)
                        .ToList();

                    if (createdFiles.Any())
                    {
                        foreach (var file in createdFiles)
                        {
                            double sizeKB = file.Length / 1024.0;
                            Console.WriteLine($"  {file.Name}");
                            Console.WriteLine($"    Размер: {sizeKB:F1} KB | Создан: {file.CreationTime:HH:mm:ss}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    PrintInfo($"Не удалось получить список созданных файлов: {ex.Message}");
                }
            }
            else
            {
                PrintWarning("Сканирование прервано пользователем.");

                if (results.Any())
                {
                    try
                    {
                        string partialOutput = Path.Combine(resultsFolder, "частичные_результаты.csv");
                        await CsvExporter.ExportToCsvAsync(results, partialOutput);
                        PrintInfo($"Частичные результаты сохранены: {Path.GetFileName(partialOutput)}");
                    }
                    catch (Exception ex)
                    {
                        PrintError($"Не удалось сохранить частичные результаты: {ex.Message}");
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            PrintWarning("Сканирование отменено.");
        }
        catch (Exception ex)
        {
            PrintError($"Критическая ошибка: {ex.Message}");
            Logger.LogError($"Критическая ошибка: {ex}");
        }

        PrintSection("ЗАВЕРШЕНИЕ");
        Console.WriteLine("\nНажмите любую клавишу для выхода...");
        Console.ReadKey();
    }
}