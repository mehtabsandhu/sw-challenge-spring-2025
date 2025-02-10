# These are the 5 libraries that are used in this script.
import csv
import os
from datetime import datetime
import multiprocessing
from itertools import chain


def load_data(directory):
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
    elif data_list[item_index][1] < 300:
      data_list[item_index][1] = data_list[item_index][1] * 10

  return data_list


def remove_duplicates(data_list):
  # Loops through the list index by index from the back to the front.
  index = len(data_list) - 1
  while index >= 0:

    # If the timestamp and price are the same, remove the item with the smaller size.
    if data_list[index][0] == data_list[index - 1][0]:
      if data_list[index][2] > data_list[index - 1][2]:
        del data_list[index - 1]
      else:
        del data_list[index]
    
    index -= 1

  return data_list


def main():

  # Creates a pool of processes to run the script in parallel (one process per CPU core).
  pool = multiprocessing.Pool(processes=os.cpu_count())

  # Loads the data from the files, cleans it, and removes duplicates.
  data_list = pool.map(load_data, os.listdir('data'))
  data_list = pool.map(remove_duplicates, data_list)
  data_list = pool.map(clean_data, data_list)

  # Closes the pool of processes, and waits for them to finish.
  pool.close()
  pool.join()

  # Flattens the list of lists into a single list.
  data_list = list(chain(*data_list))

  print(len(data_list))


if __name__ == '__main__':
  main()