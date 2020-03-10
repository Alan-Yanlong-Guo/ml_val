# DataHub

DataHub is a PYTHON package serving the data querying inside Dr. Xiu's RiskLab. It contains local copies of multiple external data vendors (i.e., CRSP) and in-house constructed data sets for academic purposes on portfolio and risk management.

* The package is developed for Python3.
* The data can be queried only in UChicago campus network (wired or wifi) or through UChicago/Booth VPN.
* Data are not allowed to be shared outside Dr. Xiu's group or used for purposes not for the group research activites.


## Install

Clone this repository into your working folder (containing python project codes).

```
git clone git@github.com:xiubooth/DataHub.git
```

## User Guide

**Import the package**

```
>>> import DataHub as hub
```

All datasets are labeled with **Handle** and **Request**. **Handle** refers the data source (i.e., vendor name or data product name), while **Request** refers the data table or view. For example, a combination of `CRSP.DailyStock` is used to query the daily stock/security files from CRSP.

**API References**

* DataHub.list_handles()
* DataHub.list_requests(HandleName)
* DataHub.show_request(HandleName, RequestName)

* DataHub.Handle
* DataHub.Handle.create(HandleName)
* Handle.read(RequestName, **args)

**List all provided handle names**
```
>>> hub.list_handles()

List of available handles:

 * CRSP
 * Compustat
 * TAQ

['CRSP', 'Compustat', 'TAQ']
```

**List all available requests to a handle (i.e., CRSP)**
```
>>> hub.list_requests('CRSP')

List of available requests in [CRSP]:

 * DailyStock     Daily data for a US stock with the given PERMNO
 * MonthlyStock   Monthly data for a US stock with the given PERMNO
 * NameHistory    Historical descriptive information for a US stock with given PERMNO
 * DelistHistory  Historical data of delisting information for US stocks with given date range
 * CompustatLink  Link table for PRMNO and Compustat-GVKey for the given date range

['DailyStock', 'MonthlyStock', 'NameHistory', 'DelistHistory', 'CompustatLink']
```

**Dataset can be queried in two steps:**

1. Create a handle linking to a data source.

2. Read data from the handle using a certain request with corresponding arguments.

**Create a handle**
```
>>> h = hub.Handle.create('CRSP')
>>> h
<DataHub.handle_mysql.Handle_MySQL object at 0x7f88878f1f98>
```

**Read data by a request**
```
>>> df = h.read('NameHistory')
>>> df
   permno    namedt  nameendt  shrcd  exchcd  ...    compno issuno hexcd hsiccd     cusip
0   10002  19860110  19930929     10       3  ...  60007907  10399     3   6020  05978R10
1   10002  19930930  19990630     11       3  ...  60007907  10399     3   6020  05978R10
2   10002  19990701  20020514     11       3  ...  60007907  10399     3   6020  05978R10
3   10002  20020515  20040609     11       3  ...  60007907  10399     3   6020  05978R10
4   10002  20040610  20060629     11       3  ...  60007907  10399     3   6020  05978R10
5   10002  20060630  20130215     11       3  ...  60007907  10399     3   6020  05978R10

[6 rows x 21 columns]

```

**Read data with additional arguments**

```
>>> df = h.read('DailyStock', fields='date,prc,ret', permno=17778, start=19960101, end=19960110)
>>> df
       date      prc       ret
0  19960102  31500.0 -0.018692
1  19960103  30925.0 -0.018254
2  19960104  30500.0 -0.013743
3  19960105  30000.0 -0.016393
4  19960108  30175.0  0.005833
5  19960109  30000.0 -0.005799
6  19960110  30000.0  0.000000

```

#### Usage: Handle.read(RequestName:str, **args)

*Return*: Pandas dataframe object

Similar to SQL query, the **RequestName** serves like the name of the target data table, while other arguments are used for selecting rules. To find out the usage of __**args__ for a certain request, use the API **DataHub.show_request(HandleName:str, RequestName:str)** to print the built-in detailed usage manual for this request.

```
>>> hub.show_request('CRSP', 'DailyStock')

Request: CRSP.DailyStock
Daily data for a US stock with the given PERMNO

Arguments:

 fields  (str) comma separated column names and/or SQL expressions
         Default: date,prc,ret

 permno  (int) PERMNO
         Default: 17778

 start   (date/int) start of date range
         Default: 19960101

 end     (date/int) end of date range
         Default: 19960110


Fields:

 * permno   NUM   CRSP Permanent Number
 * date     NUM   Date
 * ask      NUM   Closing Ask
 * askhi    NUM   Ask or High
 * bid      NUM   Closing Bid
 * bidlo    NUM   Bid or Low
 * numtrd   NUM   Number of Trades
 * openprc  NUM   Open Price
 * prc      NUM   Price
 * ret      NUM   Holding Period Return
 * retx     NUM   Return without Dividends
 * vol      NUM   Share Volume

- https://wrds-web.wharton.upenn.edu/wrds/ds/crsp/stock_q/dsf.cfm
```

A list of arguments will be printed showing the keys, types, roles and default values. If a `fields` argument is supported by this request, a list of all fields (columns) names will be also provided.

The `fields` argument (if supported) can be a comma separated column names or expressions like SQL. For examples, `COUNT(*)`, `MAX(prc)`.

**Special format of arguments**:

* **data/int** in DataHub, all dates are presented with an 8-digit integer numbers as "YYYYMMDD".

* **set/str** indicates the argument should be formatted as a set of comma separated values wrapped by round brackets. For examples, `(1,2,3)`, `('A','B')`.








