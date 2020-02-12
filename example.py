import Sqplite as sqp

database = sqp.Sqplite('Mydatabase.db' ,onOpen = sqp.createTable(name = 'students', fields = 
    [sqp.CharField(name = 'name'), sqp.IntField(name = 'age')] ))
while True:
    choice = int(input('''
        1. Enter new detail 
        2. Retrieve all results
        3. Exit
        4. Search
        5. Update
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
            print(database.query('students', where = sqp.Any([sqp.Field('name').isLike(searchfor), sqp.Field('age').isGreaterThan(5)])))
        elif searchby == 2:
            print(database.query('students', where = sqp.Field('age').isEqualTo(searchfor)))

    elif choice == 5:
        updatewherename = input('name of the record to be updated : ')
        newage = int(input('new age : '))
        database.update('students', newValue = {'age' : newage}, where = sqp.Field('name').isEqualTo(updatewherename))     