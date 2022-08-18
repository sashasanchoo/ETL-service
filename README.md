# ETL-service
It's ETL service which allows you to process files with payment transactions that different users save in the specific folder (A) on the disk 
(the path has been specified in the config). Users can save files at any. A file extension is TXT.

Example (raw_data.txt):
John, Doe, “Mykolaiv, Kleparivska 35, 4”,  500.0, 2022-27-01, 1234567, Water
Mike, Wiksen, “Dnipro, Kleparivska 40, 1”,  720.0, 2022-27-05, 7654321, Heat
Nick, Potter, “Kyiv, Gorodotska 120, 3”,  880.0, 2022-25-03, “3334444”, Parking
Luke Pan,, “Odessa, Gorodotska 120, 5”,  40.0, 2022-12-07, 2222111, Gas

When the file is processed the service saves the results in a separate folder (B) (the path is specified in the config) in a subfolder (C) with the current date.

At the end of the day (midnight) the service stores in the subfolder (C) a file called “meta.log”. The file have the following structure:
found_errors: Z
invalid_files: 
path1
path2
