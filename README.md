# Aggregator

AggregatorはQoE値を、時、日、service、country、subdivison、ispごとに集計するnodeアプリケーションです。

以下の4つのモードがあります。

-   daemon

    デーモンとして動作し、以下のモードの処理を定期的に行います。

-   update

    update モードは、DBのコレクション(sodium_collection)のremote_addressを取得し、MaxMindのDBで各値を検索し、service、country、subdivison、ispフィールドを追加します。

-   insert

    insert モードは、DBのコレクション(sodium_collection)を時、日、service、country、subdivison、ispごとに集計します。

-   remove

    remove モードは、DBのコレクション(sodium_collection)内のQoE値の取得にタイムアウトしたドキュメントを削除します。

# 集計カテゴリ

-   サービス 

    TVer、Paravi、Youtubeなど

-   地域

    -   国  

        'JP' などの2文字のアルファベット

    -   都道府県

        13(東京) などのコード

-   ISP

    'Internet Initiative Japan'などのサービス名

-   時間
    -   日  日付
    -   時  時刻

# データ欠損について

サービス、国、都道府県、ISP データは欠損している可能性があります。

上記の欠損データは、通常のデータとして取り扱うため以下の値を欠損値とします。

| カテゴリ | 値       |
| ---- | ------- |
| サービス | unknown |
| 国    | --      |
| 都道府県 | 0       |
| ISP  | unknown |

# 使用フレームワーク

-   MongoDB

# 動作確認環境

-   OS: Ubuntu 16.04
-   Node: v8.12.0

# 構成

    Aggregator  <--->  MongoDB

# インストールと起動
```
$ cd Aggregator
$ npm install
$ ./app.js -h
Usage: app $ [mode]

Options:
  -v --version         output the version number
  -t --db_type [type]  DB Type default[mongodb]
  -u --db_url [url]    MongoDB URL default[mongodb://localhost:27017]
  -n --db_name [name]  DB Type default[sodium]
  -h, --help           output usage information

Commands:
  daemon|d [options]   daemon mode: monitors the DB, adds fields, and performs aggregation.
  update|u [options]   update mode: generate location, isp, service type field
  insert|i [options]   insert mode: aggregation value insert into DB
  remvoe|r [options]   remvoe mode: remove timeout entry from DB
```

# 設定ファイル

設定ファイルは、jsonフォーマットです。config/default.jsonを書き換えることで変更することができます。 設定は、nodeのconfigモジュールを使用しています。起動時の環境変数を変更することで設定をオーバーライドすることができます。

<https://github.com/lorenwest/node-config>

    {
      "db": {
        "type": "mongodb",                              <- DBの種類(現在はMongoDB固定)
        "url": "mongodb://localhost:27017",             <- DBのURL
        "name": "sodium"                                <- DB名(現在はsodium固定)
        "aggregation_collection_prefix": "aggr_",       <- 各集約コレクションのプレフィックス
        "aggregation_collection_suffix": {
        "day": "day",                                   <- 各集約コレクションのサフィックス
        "hour": "hour",
        "service": "service",
        "isp": "isp",
        "country": "country",
        "subdivision": "subdivision"
      },
      "qoe": {
        "timeout": 86400000                             <- QoE値取得タイムアウト(ミリ秒)
      },
      "interval": {
        "aggregate": 86400000,                          <- 集計対象ドキュメントのstart_timeの範囲(ミリ秒)
                                                           上記の設定では、現在から1日さかのぼって集計対象としている
        "update": 10000,                                <- service、country、subdivison、isp のフィール更新間隔(ミリ秒)
        "insert": 300000,                               <- 集計コレクションの更新間隔(ミリ秒)
        "remove": 600000                                <- QoE値取得にタイムアウトしたドキュメントの削除間隔(ミリ秒)
      },
      "file": {
        "city": "./location_data/GeoIP2-City.mmdb",     <- MaxMindのDBファイル
        "ISP": "./location_data/GeoIP2-ISP.mmdb"
      }
    }

# オプション

以下のオプションは、上記の設定ファイルの値を上書きします。

    Usage: app $ [mode]

    Options:
      -v --version         output the version number
      -t --db_type [type]  DB Type default[mongodb]
      -u --db_url [url]    MongoDB URL default[mongodb://localhost:27017]
      -n --db_name [name]  DB Type default[sodium]
      -h, --help           output usage information

    Commands:
      daemon|d [options]   daemon mode: monitors the DB, adds fields, and performs aggregation.
      update|u [options]   update mode: generate location, isp, service type field
      insert|i [options]   insert mode: aggregation value insert into DB
      remvoe|r [options]   remvoe mode: remove timeout entry from DB

## daemon モード

daemon モードは、デーモンとして動作し、以下のモードの処理を定期的に行います。--update_interval、--insert_interval、--remove_intervalで指定した間隔で処理を実行します。また、--aggregate_intervalは、対象のドキュメントを検索する際に使用し、デフォルトでは、処理開始時刻の1日前から処理開始時刻までの値に視聴を開始したドキュメントに対して行われます。

    daemon mode: monitors the DB, adds fields, and performs aggregation.

    Options:
      -c --source_collection [collection_name]     Name of collection that storing from Extension default[sodium_collection]
         --aggregation_collection_prefix [prefix]  prefix of aggregation collection name. default[aggr_]
         --remove_collection [collection_name]     Name of collection to remove timeout entry. default[sodium_collection]
         --aggregate_interval [millisec]           aggregate interval default[86400000]
         --update_interval [millisec]              DB update interval default[10000]
         --insert_interval [millisec]              DB insert interval default[300000]
         --remove_interval [millisec]              DB remove interval default[600000]
         --city_file [city_file]                   Maxmind city file default[./location_data/GeoIP2-City.mmdb]
         --isp_file [isp_file]                     Maxmind ISP file default[./location_data/GeoIP2-ISP.mmdb]
      -t --timeout [MILLISECOND]                   QoE Server timeout. default 24h [86400000]
      -h, --help                                   output usage information

## update モード

update モードは、DBのコレクション(sodium_collection)のremote_addressを取得し、MaxMindのDBで各値を検索し、service、country、subdivison、ispフィールドを追加します。
\--start_time、--end_timeオプションで時刻を指定すると範囲内のドキュメントを対象に更新処理を行います。また、--remove_fieldsを指定すると追加したフィールドをすべて削除します。

    Usage: update|u [options]

    update mode: generate location, isp, service type field

    Options:
      -c --source_collection [collection_name]  Name of collection that storing from Extension default[sodium_collection]
      -s --start_time [DATE_TIME]               start_time filed query(gte) for update. format ISO_8601 defalut[1970-01-01T09:00:00+09:00]
      -e --end_time [DATE_TIME]                 start_time filed query(lt) for update. format ISO_8601 defalut[now]
         --city_file [city_file]                Maxmind city file default[./location_data/GeoIP2-City.mmdb]
         --isp_file [isp_file]                  Maxmind ISP file default[./location_data/GeoIP2-ISP.mmdb]
         --remove_fields                        remove update fields default[false]
      -h, --help                                output usage information

## insert モード

insert モードは、DBのコレクション(sodium_collection)を時、日、service、country、subdivison、ispごとに集計します。集計結果は各コレクションに追加されます。--start_time、--end_timeオプションで時刻を指定すると範囲内のドキュメントを対象に集計処理を行います。また、--remove_aggregation_collectionを指定すると集計結果をすべて削除します。

作成されるコレクション
| 名前 | 値 |
|---|---|
| aggr_day                  |日毎の集計結果|
| aggr_hour                 |時毎の集計結果|
| aggr_service_day          |service、日毎の集計結果|
| aggr_service_hour         |service、時毎の集計結果|
| aggr_country_day          |country、日毎の集計結果|
| aggr_country_hour         |country、時毎の集計結果|
| aggr_subdivision_day      |subdivision、日毎の集計結果|
| aggr_subdivision_hour     |subdivision、時毎の集計結果|
| aggr_isp_day              |isp、日毎の集計結果|
| aggr_isp_hour             |isp、時毎の集計結果|

    Usage: insert|i [options]

    insert mode: aggregation value insert into DB

    Options:
      -c --source_collection [collection_name]     Name of collection to aggregation. default[sodium_collection]
      -s --start_time [DATE_TIME]                  start_time filed query(gte) for aggregation. format ISO_8601 defalut[1970-01-01T09:00:00+09:00]
      -e --end_time [DATE_TIME]                    start_time filed query(lt) for aggregation. format ISO_8601 defalut[now]
         --aggregation_collection_prefix [prefix]  prefix of aggregation collection name. default[aggr_]
         --remove_aggregation_collection           remove aggregation collection. default[false]
      -h, --help                                   output usage information

## remove モード

remove モードは、DBのコレクション(sodium_collection)内のQoE値の取得にタイムアウトしたドキュメントを削除します。--timeoutでタイムアウト時間を指定します。

    Usage: remvoe|r [options]

    remvoe mode: remove timeout entry from DB

    Options:
      -t --timeout [MILLISECOND]                QoE Server timeout. default 24h [86400000]
         --remove_collection [collection_name]  Name of collection to remove timeout entry. default[sodium_collection]
      -h, --help                                output usage information
