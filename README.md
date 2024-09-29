# `ahitool`

A CLI tool serving the miscellaenous needs of REDACTED roofing company.
Capabilities include processing sales perfomance analytics, financial
information, spreadsheets, and geolocation. `ahitool` connects to a JobNimbus
database to access roofing data, and uses OAuth2 with Google to automatically
generate Google Sheets of results.

# subcommands

## generate key performance indicators

```
ahitool kpi [OPTIONS]
```

`ahitool` can process a bulk data set of jobs to determine the rates at which a
job progresses from a simple sales lead to a completed installation. This
includes the percentage of jobs that manage to successfully reach a certain
milestone, as well as the average time taken to reach that milestone. These "key
performance indicators" are calculated globally for the entire sales department,
as well as for each sales representative individually.

## list accounts receivable

```
ahitool ar [OPTIONS]
```

`ahitool` can process a bulk data set of jobs to aggregate and list all the
accounts receivable to the company. These accounts receivable are segregated and
aggregated by the status of the associated job.

Some accounts receivable will have $0 amounts. This is because the jobs not
filtered by the amount receivable, but by their status. This is the client's
preferred behavior.

## find similar jobs in a certain area

This subcommand is work-in-progress. In order to expedite the process of getting
customers familiar with the standard of quality upheld by the client, `ahitool`
will be able to find and report all jobs with a certain roof type and color
within a certain mile radius of a given zip code or address.

# output format

Different output formats can be specified using the `--format` option.
- `--format human` will print a pretty, human-readable file/set of files to the
  file/directory specified by `--output`.
- `--format csv` will print a (set of) CSV file to the file/directory specified
  by `--output`.
- `--format google-sheets` will prompt the user to authorize `ahitool` with
their Google account, and then automatically generate a Google Sheet containing
the results. The authorization is cached in the current working directory's
`google_oauth_token.json` file so that it can be reused. without prompting
again.

# JobNimbus API key

For all current functionalities, `ahitool` requires access to the JobNimbus API.
To grant `ahitool` access, simply
[generate](https://support.jobnimbus.com/how-do-i-create-an-api-key) and provide
a JobNimbus API key. In the first invocation, the key can be supplied via the
`--jn-api-key` option or the `JN_API_KEY` variable; this key will be cached in
the current working directory's `job_nimbus_api_key.txt` for future invocations.
