# These are the 6 libraries that are used in this script.
import csv
import os
from datetime import datetime, timedelta
import multiprocessing
from itertools import chain
from functools import partial


def load_data(directory):
  """
  Loads all the rows from a provided csv file into a list
  Skips over rows that contain empty (missing) data
  Also ensures that no data is loaded from the wrong day

  :param directory: the name of the csv file in the 'data' folder
  :return: a list containing all the rows in the provided csv file
  """

  # An empty list to store the data from the file.
  data_list = []

  # Opens the file and reads the data into the list row by row.
  with open(os.path.join('data', directory), 'r') as f:
    reader = csv.reader(f)
    for row in reader:
      # Skips empty rows
      if "" in row:
        continue

      # Skips the header row
      if (row[0] == 'Timestamp'):
        continue

      # The following code makes sure no data is loaded from the wrong day.
      # It compares the date in the filename to the date in the data.
      if int(directory[9:17]) != int(row[0][0:4] + row[0][5:7] + row[0][8:10]):
        continue

      # Appends the row to the list.
      data_list.append(row)

  return data_list


def clean_data(data_list):
  """
  Cleans up the data from the provided list of stock ticker entries
  The timestamps are converted to datetime format, the prices are converted to floats, and the sizes are converted to ints
  If the price is negative, the sign is changed to positive
  If the price is an outlier, likely due to a misplaced decimal point (e.g. 41.72 instead of 417.2), multiplies by 10 to correct

  :param data_list: a raw list of stock ticker entries to be cleaned
  :return: the cleaned up list of stock ticker entries
  """

  # Loops through all the items in the list, cleaning it up.
  for item_index in range(len(data_list)):

    # Convert the string formatted timestamp to a datetime object.
    data_list[item_index][0] = datetime.strptime(data_list[item_index][0], '%Y-%m-%d %H:%M:%S.%f')

    # Convert the string formatted price to a float.
    data_list[item_index][1] = float(data_list[item_index][1])

    # Convert the string formatted size to an integer.
    data_list[item_index][2] = int(data_list[item_index][2])

    # If the price is negative, make it positive.
    if data_list[item_index][1] < 0:
      data_list[item_index][1] = -data_list[item_index][1]

    # If the following item is an outlier, likely due to a misplaced decimal point, multiply by 10 to correct.
    if data_list[item_index][1] < 300:
      data_list[item_index][1] = data_list[item_index][1] * 10

  return data_list


def remove_duplicates(data_list):
  """
  Within some lists, there may be duplicate entries that have the same timestamp, yet differing prices and sizes
  In this case, we will keep the entry with the larger size (volume), and delete the other

  :param data_list: the list of stocker ticker entries that may contain duplicates
  :return: a list of stocker ticker entries that doesn't contain any duplicate timestamp entries
  """

  # Loops through the list index by index from the back to the front.
  index = len(data_list) - 1
  while index > 0:

    # If the timestamps are the same, remove the item with the smaller size (volume).
    if data_list[index][0] == data_list[index - 1][0]:
      if data_list[index][2] > data_list[index - 1][2]:
        del data_list[index - 1]
      else:
        del data_list[index]
    
    index -= 1

  return data_list


def grab_time_interval():

  # Stores a boolean indicating whether the user's input is valid
  valid_input = False
  # Stores the interval values for 'second', 'minute', 'hour', and 'day' in this dictionary
  smhd = {}
  # Prompts the user to input a time interval until a valid is provided
  while not valid_input:
    time_interval = input("Enter the desired time interval: ").lower()
    char_index = 0    # To store the index of the last char
    current_index = 0 # To store the current index we are iterating through
    smhd = {}         # Resets the dictionary to an empty one
    for char in time_interval:
      # If the current character isn't an integer, or isn't 's', 'm', 'h', or 'd' to indicate time intervals,
      # prompts user to input time interval again
      if char not in 'smhd0123456789':
        print("Invalid input. Please enter a valid time interval.")
        valid_input = False
        break
      if char in 'smhd':
        # if the valid letter character is the first character in the string, or doesn't come directly after an integer,
        # prompts user to input time interval again
        if current_index == 0 or current_index == char_index:
          print("Invalid input. Please enter a valid time interval.")
          valid_input = False
          break
        # else if the valid letter character comes after integer(s), sets the interval value as the numbers that come before it
        # The char_index is also incremented accordingly
        valid_input = True
        smhd[char] = int(time_interval[char_index:current_index])
        char_index = current_index + 1
      # the current_index is incremented regardless of the character
      current_index += 1
  if 's' not in smhd:
    smhd['s'] = 0
  if 'm' not in smhd:
    smhd['m'] = 0
  if 'h' not in smhd:
    smhd['h'] = 0
  if 'd' not in smhd:
    smhd['d'] = 0
  return smhd


def grab_timeframe():
  valid_start = False
  valid_end = False
  timeframe = {}

  while not valid_start:
    start_date = input("Enter the start datetime in the following format (YYYY-MM-DD): ")
    try:
      start_date = datetime.strptime(start_date, '%Y-%m-%d')
      valid_start = True
      timeframe['start'] = start_date
    except ValueError:
      print("Invalid input. Please enter a valid date.")
  
  while not valid_end:
    end_date = input("Enter the end datetime in the following format (YYYY-MM-DD): ")
    try:
      end_date = datetime.strptime(end_date, '%Y-%m-%d')
      valid_end = True
      timeframe['end'] = end_date
    except ValueError:
      print("Invalid input. Please enter a valid date.")
  
  return timeframe


def generate_timeframe_intervals(time_intervals, timeframes):
  timeframe_interval_list = []

  current_timeframe = timeframes['start']

  while current_timeframe < timeframes['end']:
    list_to_add = [current_timeframe]
    current_timeframe = current_timeframe + timedelta(seconds=time_intervals['s'], minutes=time_intervals['m'], hours=time_intervals['h'], days=time_intervals['d'])
    list_to_add.append(current_timeframe)
    timeframe_interval_list.append(list_to_add)
  
  timeframe_interval_list[-1][1] = timeframes['end']

  return timeframe_interval_list


def generate_ohlcv(list_of_data, timeframe_interval):
  start_index = 0

  left = 0
  right = len(list_of_data) - 1
  mid = 0
  foundMid = False

  while left <= right:
    mid = left + (right - left) // 2

    if list_of_data[mid][0] == timeframe_interval[0]:
      start_index = mid
      foundMid = True
    elif list_of_data[mid][0] < timeframe_interval[0]:
      left = mid + 1
    else:
      right = mid - 1
  
  if not foundMid:
    start_index = mid

  ohlcv = [
    timeframe_interval[0],   # Timestamp
    list_of_data[start_index][1],  # Open
    list_of_data[start_index][1],  # High
    list_of_data[start_index][1],  # Low
    None, # Close
    0,    # Volume
  ]

  while start_index < len(list_of_data) and list_of_data[start_index][0] < timeframe_interval[1]:
    ohlcv[2] = max(ohlcv[1], list_of_data[start_index][1]) # High
    ohlcv[3] = min(ohlcv[2], list_of_data[start_index][1]) # Low
    ohlcv[5] += list_of_data[start_index][2]               # Volume
    start_index += 1
  
  ohlcv[4] = list_of_data[start_index - 1][1] # Close

  return ohlcv


def generate_flat_file(ohlcv_list, timeframe):
  os.makedirs('ohlcv_flat_files', exist_ok=True)
  with open(os.path.join('ohlcv_flat_files', 'ohlcv_{start}_{end}.csv'.format(start=str(timeframe['start'])[:10], end=str(timeframe['end'])[:10])), 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
    for ohlcv in ohlcv_list:
      writer.writerow(ohlcv)
  

def main():
  # Creates a pool of processes to run the script in parallel (one process per CPU core).
  data_loading_pool = multiprocessing.Pool(processes=os.cpu_count())

  # Loads the data from the files, cleans it, and removes duplicates.
  data_list = data_loading_pool.map(load_data, os.listdir('data'))
  data_list = data_loading_pool.map(remove_duplicates, data_list)
  data_list = data_loading_pool.map(clean_data, data_list)

  # Closes the pool of processes, and waits for them to finish.
  data_loading_pool.close()
  data_loading_pool.join()

  # Flattens the list of lists into a single list.
  data_list = list(chain(*data_list))

  # Grabs the time interval and timeframe from the user.
  time_intervals = grab_time_interval()
  timeframe = grab_timeframe()

  # Generates the timeframe intervals to create the OHLCV records.
  timeframe_intervals = generate_timeframe_intervals(time_intervals, timeframe)

  # Creates a pool of processes to run the script in parallel (one process per CPU core).
  ohlcv_pool = multiprocessing.Pool(processes=os.cpu_count())

  # Creates a partial function to pass the data_list to the generate_ohlcv function.
  # This is required to use ohlcv_pool.map() afterwards
  generate_ohlcv_partial = partial(generate_ohlcv, data_list)

  # Generates the OHLCV records for each timeframe interval.
  ohlcv_list = ohlcv_pool.map(generate_ohlcv_partial, timeframe_intervals)

  # Closes the pool of processes, and waits for them to finish.
  ohlcv_pool.close()
  ohlcv_pool.join()

  # Prints the OHLCV records.
  for ohlcv in ohlcv_list:
    print(ohlcv)
  
  # Creates the csv flat file containing the ohlcv bars
  generate_flat_file(ohlcv_list, timeframe)
  

if __name__ == '__main__':
  main()