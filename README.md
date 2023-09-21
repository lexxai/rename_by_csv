# txt2csv

Read txt files from input folders, and store rows of txt file as columns records to csv file.
To skip the header, specify --headers an empty string ""

```
usage:  [-h] [-V] [--work WORK] --input INPUT [--output OUTPUT] [--headers HEADERS] [--verbose]

options:
  -h, --help         show this help message and exit
  -V, --version      show version of app
  --work WORK        Directory for work. Is prefix for all other directories that is not absolute, default ''
  --input INPUT      Path to input folder
  --output OUTPUT    Path for output file, default 'output.csv'
  --headers HEADERS  The header for the csv file. To skip the header, specify an empty string "". Default: "FILENAME,ID,NAME,FIELD1,FIELD2,FIELD3,FIELD4"
  --verbose          verbose output

```
