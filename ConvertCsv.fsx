#r "nuget: FSharp.Data, Version=6.4.0"

open System
open System.IO
open System.Net.Http
open System.Text.Json

open FSharp.Data

type KrakenCsv = CsvProvider<"kraken_ledgers_export_snip.csv">
type KrakenTargetCsv = CsvProvider<"kraken_ledgers_export_target.csv">

type BitStampCsv = CsvProvider<"BitstampTransactionsExport_snip.csv">
type BitStampTargetCsv = CsvProvider<"BitstampTransactionsExport_target.csv">

let getBTCPriceInEUR (date: DateOnly): decimal =
    async {
        let dateFormated = date.ToString("yyyyMMdd")
        use httpClient = new HttpClient()
        if date = (DateTime.Today |> DateOnly.FromDateTime) then
            let uri = Uri "https://cex.io/api/last_price/BTC/EUR"
            let! response = httpClient.GetStringAsync uri |> Async.AwaitTask
            let json = JsonDocument.Parse response
            return json.RootElement.GetProperty("lprice").GetString() |> decimal
        else
            let baseUrl = $"https://cex.io/api/ohlcv/hd/{dateFormated}/BTC/EUR"
            let uri = Uri baseUrl
            let task = httpClient.GetStringAsync uri
            let! response = Async.AwaitTask task
            let json = JsonDocument.Parse response
            let dataString = json.RootElement.GetProperty("data1d").GetString()
            let dataJsonArray = (JsonValue.Parse dataString).AsArray()
            let ohlcv = 
                dataJsonArray 
                |> Array.map (fun each -> each.AsArray()) 
                |> Array.find (fun each -> 
                    let currDate = 
                        DateTime.UnixEpoch.AddSeconds(each.[0].AsInteger()).Date 
                        |> DateOnly.FromDateTime
                    currDate = date)
            let openValue = ohlcv.[1].AsDecimal()
            let closeValue = ohlcv.[4].AsDecimal()
            return (openValue + closeValue) / 2.0m
    }
    |> Async.RunSynchronously

let todaysPrice = lazy(getBTCPriceInEUR (DateOnly.FromDateTime DateTime.Today))

let getModifiedFileName (file: FileInfo) =
    let nameWithoutExtension = file.Name.Substring(0, file.Name.Length - file.Extension.Length)
    nameWithoutExtension + "_modified" + file.Extension

let args = Environment.GetCommandLineArgs()

let csvType = args.[2]
let file = FileInfo args.[3]

if csvType.ToLower() = "kraken" then
    let csv = KrakenCsv.Load file.FullName
    let modifiedCsv = 
        csv.Filter(fun row -> row.Type = "trade" && row.Amount > 0.0m)
    let targetRows = 
        modifiedCsv.Rows
        |> Seq.map (fun row ->
            let avgBTCValueOnTxDate = getBTCPriceInEUR (DateOnly.FromDateTime row.Time)
            let amountInEURToday = row.Amount * todaysPrice.Value
            let amountInEURSpent = row.Amount * avgBTCValueOnTxDate
            KrakenTargetCsv.Row(
                row.Txid, row.Refid, row.Time, row.Type, row.Subtype, row.Aclass, row.Asset, row.Wallet, row.Amount, row.Fee, row.Balance,
                avgBTCValueOnTxDate, amountInEURSpent, amountInEURToday) )
    let targetCsv = new KrakenTargetCsv(targetRows)
    targetCsv.Save(Path.Join(file.DirectoryName, getModifiedFileName file))
elif csvType.ToLower() = "bitstamp" then
    let csv = BitStampCsv.Load file.FullName
    let modifiedCsv = csv.Filter(fun row -> row.``Amount currency`` = "BTC")
    let targetRows = 
        modifiedCsv.Rows
        |> Seq.map (fun row ->
            let avgBTCValueOnTxDate = getBTCPriceInEUR (DateOnly.FromDateTime row.Datetime.DateTime)
            let amountInEURToday = row.Amount * todaysPrice.Value
            let amountInEURSpent = row.Amount * avgBTCValueOnTxDate
            BitStampTargetCsv.Row(
                row.ID, row.Type, row.Subtype, row.Datetime, row.Amount, row.``Amount currency``, row.Value, row.``Value currency``, row.Rate, row.``Rate currency``, row.Fee, row.``Fee currency``, row.``Order ID``,
                avgBTCValueOnTxDate, amountInEURSpent, amountInEURToday) )
    let targetCsv = new BitStampTargetCsv(targetRows)
    targetCsv.Save(Path.Join(file.DirectoryName, getModifiedFileName file))
else
    failwithf "Unknown type: %s (valid types: kraken, bitstamp)" csvType
