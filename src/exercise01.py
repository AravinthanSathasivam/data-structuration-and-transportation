file_path = '../resources/plain/months.txt'

with open(file_path, 'r') as file:
    for line in file:
        print(line, end='')
