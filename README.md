# Kynetec Brazil - Data Acquisition
---

Set of scripts to facilitate the acquisition of primary and public data, from public APIs or local files, to enrich kynetec data and publish products such as Farmtrak Distribution.

__Available processes:__

* [Kynetec Raw CSV](#kynetec-raw-csv)
* [SIDRA - IBGE](#sidra---ibge)
* [Conab](#conab)

---
### Installation
---

__1. Clone this repository__
```
git clone git@github.com:kynetec-brasil/data_acquisition.git
```

__2. Create environment and install dependencies__

Windows ([conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/windows.html)):
```
$ conda create -n your_env python
$ conda activate your_env
(your_env) $ python -m pip install --upgrade pip wheel
(your_env) $ python -m pip install -r requirements.txt
```

Linux:
```
$ python -m venv --clear --copies your_env
$ source your_env/bin/activate
(your_env) $ python -m pip install --upgrade pip wheel
(your_env) $ python -m pip install -r requirements.txt
```

__3. Create `.env` file__

Create a `.env` file at the root of the project following the `.env-sample` variables.
 
---
### Usage
---

#### Kynetec Raw CSV

Tool created to upload raw CSVs to the Farmtrak Distribution Database.

```
$ cd kynetec
$ python kynetec_uploader.py -f <csv_file_path> -s <source*>

-f, --file: Path to the csv file to be uploaded/updated.
-s, --source: Database source to be updated.
```
*source values represent the entity in the farmtrak distribution to be updated, the availables source values can be found as keys in `config_files.json` at kynetec folder.

__Optional parameters__
```
-c, --create: Create rawdata schema and tables in database.
-r, --reset: Reset (truncate) table before upload.
--sep <value>:  CSV separator, default ';'.
--encoding <value>: CSV encoding, default 'latin1'.
```

__Example__
```
$ cd kynetec
$ python kynetec_uploader.py -c -f BASE_LOCAL_COMPRA.csv -s base_local_compra
```
---
#### SIDRA - IBGE

Tool created to download and load IBGE data requested from SIDRA (*Sistema IBGE de Recuperação Automática*) to Farmtrak Distribution database.

```
$ cd sidra
$ python sidra_downloader.py -d <date*>

-d, --date: Date or date range to be downloaded.
```
*date value can be any accepted by SIDRA API, such as:
```
* 2022 (one year)
* 2017,2020,2022 (many years)
* 2016:2022 (range of years)
* 2000,2007:2020 (list and range of years)
* first 10 (first 10 available years)
* last 10 (last 10 available years)
* all (all available years)
```

__Optional parameters__
```
-t, --table <value*, ...>: List of tables to be downloaded, if not specified all tables will be downloaded
-c, --create: Create rawdata schema and tables in database.
-r, --reset: Reset (truncate) table before upload.
--debug: Debug mode, will download only for one municipality to test the process
```
*table values are available as keys in `sidra_catalog.json` at sidra folder.

__Example__
```
$ cd sidra
$ python sidra_downloader.py -c -d 2021 -t pevs_291 ppm_74 --debug
```
---
#### CONAB

Tool created to download and load CONAB data requested from CONAB (*Portal de Informações Agropecuárias*) to Farmtrak Distribution database.

```
$ cd conab
$ python conab_downloader.py
```

__Optional parameters__
```
-t, --table <value*, ...>: List of tables to be downloaded, if not specified all tables will be downloaded
-r, --reset: Reset (truncate) table before upload.
```
*table values are available as keys in `conab_catalog.json` at conab folder.

__Example__
```
$ cd conab
$ python conab_downloader.py -r -t conab_graos
```
---
