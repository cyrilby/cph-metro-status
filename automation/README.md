# Setting up automated data collection on Linux

* Author: [github.com/cyrilby](github.com/cyrilby)
* Last meaningful update: 08-07-2024

This document briefly describes how to set up automatic data collection of CPH metro data on a Linux OS using the built-in `crontabs` function, which can be used to run e.g. Python scripts on set intervals.

## Open the crontab file to modify it

In this first step, open the **Terminal**, then type this command to open the `crontab` service:

```
crontab -e
```

Navigation in the `crontab` is solely carried out by using the keyboard. This applies to both modifying existing entries, adding new entries and saving the file.

## Set up a script running every 10 minutes

In the example below, we setp up a script that runs automatically every 10 minutes, starting at 11:40 o'clock and repeating indefinitely:

```
40,50 11 * * * /home/admin/Documents/Automation/run_cph_scraper_data_collection.sh
```

The exact command of what is to be done is contained in the bash script and can look like:

```
#!/bin/bash
cd "/home/admin/Documents/cph-metro-status/"
source /home/admin/Documents/cph-metro-status/metropy/bin/activate
python /home/admin/Documents/cph-metro-status/summarize_data_linux.py
```

## Set up a script running every 1 hour

Alternatively, we can have a set-up where scripts are executed on an hourly basis instead, with only one occurrence per hour. This can be achieved as follows:

```
54 * * * /home/admin/Documents/Automation/run_cph_scraper_data_cleaning.sh
```

## Save the crontab file with Ctrl+X

It is imperative to use this command as well as to confirm your choice to save the `crontab` jobs (if you're prompted to do so). Otherwise, the jobs may not be saved and the automation may need to be set up again.

## Run the following command to restart the service

To make sure the automation is activated as quickly as possible, consider restarting the `crontab` job before moving on to other tasks:

```
sudo service cron restart
```

That's it! 😊

## P.S.

This approach has only been tested on Raspberry Pi OS and may need modifications to work on other Linux systems.