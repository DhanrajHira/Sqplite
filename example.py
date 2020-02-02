import Sqplite as sqp 

database = sqp.Sqplite('Mydatabase.db' ,onOpen = 'students (name VARCHAR(50), age INTEGER)')
while True:
    choice = int(input('''
        1. Enter new detail 
        2. Retrieve all results
        3. Exit
        4. Search
    '''))
    if choice == 1:
        name = input('Enter the name ')
        age = int(input('Enter the age '))
        data = {'name' : name , 'age' : age}
        database.insert('students', datamap = data)
    elif choice == 2:
        print(database.query('students'))
    elif choice == 3:
        break
    elif choice == 4:
        searchby = int(input('''
            1. By name
            2. By age
        '''))
        searchfor = input('Search for : ')
        if searchby == 1:
            print(database.query('students', where = 'name = \'{0}\''.format(searchfor)))
        elif searchby == 2:
            print(database.query('students', where = 'age = {0}'.format(searchfor)))