# How to use this data processing interface

After running the main file, allow a couple seconds for the program to process the csv files.

You will be prompted to enter the time interval first. A time interval is a combination of days, hours, minutes, and seconds. A valid input can consist of integers, and the letters 'd' for days, 'h' for hours, 'm' for minutes, and 's' for seconds. There can be no other characters or spaces, and each letter must have numbers preceeding it. Should your input be invalid, you will be prompted to re-enter a valid time interval.

Afterwards, you will be prompted to enter a start datetime and an end datetime in the format YYYY-MM-DD. These hyphens are necessary to include in the input. Once again, should your input be invalid, you will be prompted to re-enter a valid time interval.

# Assumptions:

It is assumed that when entering the start and end datetime, you are entering datetimes that fall within the range 2024-09-16 to 2024-09-20. It's assumed that the times you will enter are between 09:30 and 21:00. It is also assumed that the start datetime entered is chronologically before the end datetime entered.

# Limitations:

This current model does not account for times when the market is closed (e.g. after 21:00 and before 09:30).