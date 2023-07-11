import csv

class Student:
    def __init__(self, id, name, city, school):
        self.id = id
        self.name = name
        self.city = city
        self.school = school

file_path = '../resources/csv/users.csv'
students = []

with open(file_path, 'r') as file:
    reader = csv.reader(file)

    for row in reader:
        id, name, city, school = row
        student = Student(id, name, city, school)
        students.append(student)


for student in students:
    print("ID:", student.id)
    print("Name:", student.name)
