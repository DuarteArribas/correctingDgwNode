# Delete inconsistencies of the NOA's node with DWG

This script performs a series of database operations to synchronize the rinex_file table between the DGW and NOA. It gets a list of stations at NOA and for each station:

* Select files that are AT NOA with status > 0 that do not exist at DGW (no two files between nodes with the same filename) and insert at DGW with the values at NOA;
* Select files that are AT NOA with status > 0 that are different than the same file at DGW (filename is the same, but other values are different) and update at DGW with the values at NOA;
* Select files that are AT NOA with status <= 0 that exist at DGW and delete at DGW.

The script produces:

* the number of to be changed files;
* 3 SQL scripts to execute inserts, updates and deletes.

## Installation

Clone and change into the repository using `git clone https://github.com/DuarteArribas/correctingDgwNode`, `cd correctingDgwNode`. Make sure you have Python (this project was built for version 3.11.4, but older should still work) installed and pip as well.

Install the requirements in requirements.txt using `pip install -r requirements.txt`.

## Before running

Before running the program, make sure you edit the `conf.cfg` file with the appropriate IP, Database Name, Username and Password of the respective servers.

## Running

To run the program, type:

```python
python correctingDgwNode.py [-h]

h | Help : Display help and usage
```

## Troubleshooting

If you have trouble installing `psycopg2`, I did too! In that case, please also install the following if you haven't already:

```bash
sudo apt install libpq-dev python3-dev
sudo apt install build-essential
```

It worked for me [src](https://stackoverflow.com/questions/5420789/how-to-install-psycopg2-with-pip-on-python).