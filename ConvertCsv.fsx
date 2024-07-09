#r "nuget: FSharp.Data, Version=6.4.0"

open System
open System.IO
open System.Net.Http
open System.Text.Json

open FSharp.Data

[<Literal>]
let krakenCsvSource = """
"txid","refid","time","type","subtype","aclass","asset","wallet","amount","fee","balance"
"L4KKJU-NBWE3-RXGDGW","QGBYLH6-R2VQXW-57ET7W","2016-12-03 09:28:24","deposit","","currency","BTC","spot / main",0.0100000000,0.0,0.0100000000
"LNN3WX-7RQ4N-RG4GPE","T34SV6-JJE3B-BE3FOM","2016-12-03 09:57:31","trade","","currency","BTC","spot / main",-0.0099470000,0.0,0.0000530000
"LQJ73V-IZDYD-WF7QDR","QGBTNW5-TMNAGG-554WKC","2016-12-03 13:35:08","deposit","","currency","BTC","spot / main",0.1845177300,0.0,0.1845707300
"LYDRXZ-H236G-BXIMLG","TF7H5O-WSLJM-XZPW26","2016-12-03 13:55:32","trade","","currency","BTC","spot / main",-0.1561110000,0.0,0.0284597300
"""

type KrakenCsv = CsvProvider<krakenCsvSource>

[<Literal>]
let krakenTargetCsvSource = """
txid,refid,time,type,subtype,aclass,asset,wallet,amount,fee,balance,Average BTC value in EUR that day,Approx amount in EUR spent,Approx value in EUR present day
L4KKJU-NBWE3-RXGDGW,QGBYLH6-R2VQXW-57ET7W,03.12.2016 9:28,deposit,,currency,BTC,spot / main,0.01,0.0,0.01,800.0,8.0,500.0
"""
type KrakenTargetCsv = CsvProvider<krakenTargetCsvSource>

[<Literal>]
let bitStampCsvSource = """
ID,Account,Type,Subtype,Datetime,Amount,Amount currency,Value,Value currency,Rate,Rate currency,Fee,Fee currency,Order ID
351051,Main Account,Market,Buy,2018-04-19T08:14:00Z,0.19307520,BTC,25.93,USD,134.3,USD,0.13000,USD,
750710,Main Account,Market,Buy,2019-06-09T22:00:46Z,0.03594000,BTC,3.66,USD,101.9,USD,0.02000,USD,
12689837,Main Account,Market,Buy,2020-12-21T11:01:30Z,0.01905732,BTC,14.96,EUR,785,EUR,0.04000,EUR,168170217
67642304,Main Account,Market,Buy,2021-06-04T11:49:57Z,0.03760132,ETH,0.00296900,BTC,0.07895999,BTC,0.00000600,BTC,1626588334
"""

type BitStampCsv = CsvProvider<bitStampCsvSource>

[<Literal>]
let bitStampTargetCsvSource = """
ID,Order Type,Subtype,Datetime,Amount,Amount currency,Value,Value currency,Rate,Rate currency,Fee,Fee currency,Order ID,Average BTC value in EUR that day,Approx amount in EUR spent,Approx value in EUR present day
351051,Market,Buy,2018-04-19T08:14:00Z,0.1930752,BTC,25.93,USD,134.3,USD,0.13,USD,,800,8,500
67642304,Market,Buy,2021-06-04T11:49:57Z,0.03760132,ETH,0.002969,BTC,0.07895999,BTC,0.000006,BTC,1626588334,0.0,0.0,0.0
"""

type BitStampTargetCsv = CsvProvider<bitStampTargetCsvSource>

[<Literal>]
let bitcoinityCsvSource = """
Time,bit-x,bitfinex,bitstamp,cex.io,coinbase,exmo,gemini,itbit,kraken,others
2011-06-20 00:00:00 UTC,,,,,,,,,,14.0
2020-03-31 00:00:00 UTC,6451.99019022834,6457.72781466452,6451.4337806601,6460.91003570688,6452.08720718026,6743.31873439978,6453.62268276825,6451.36892455235,6451.03584674415,
"""

type BitcoinityCsv = CsvProvider<bitcoinityCsvSource, PreferOptionals=true>

let historicalBTCPriceData = lazy(
    async {
        let! csv = 
            BitcoinityCsv.AsyncLoad 
                "https://data.bitcoinity.org/export_data.csv?c=e&currency=EUR&data_type=price&t=l&timespan=all"
        return 
            seq {
                let mutable lastPrice = 0.0m
                for row in csv.Rows do
                    let date = DateOnly.Parse row.Time.[..9]
                    let struct(_, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10) = row.ToValueTuple()
                    let prices = [| p1; p2; p3; p4; p5; p6; p7; p8; p9; p10 |] |> Array.choose id
                    // there are rows with no price data, in that case return last known price
                    if prices.Length > 0 then
                        lastPrice <- (Array.sum prices) / (decimal prices.Length)
                    yield date, lastPrice
            }
            |> readOnlyDict
    }
    |> Async.RunSynchronously
)

let todaysBTCPrice = lazy(
    async {
        use httpClient = new HttpClient()
        let uri = Uri "https://cex.io/api/last_price/BTC/EUR"
        let! response = httpClient.GetStringAsync uri |> Async.AwaitTask
        let json = JsonDocument.Parse response
        return json.RootElement.GetProperty("lprice").GetString() |> decimal
    }
    |> Async.RunSynchronously
)

let getBTCPriceInEUR (date: DateOnly): decimal =
    if date = (DateTime.Today |> DateOnly.FromDateTime) then
        todaysBTCPrice.Value
    else
        historicalBTCPriceData.Value.[date]

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
