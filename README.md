# quetzal v0.4.0.1
### DBMS that plans to implement a functionality to accept any python data type, one to remotely connect to the database, an API for other languages, and a long etc.

#### My Discord: savra#0001

![quetzal shadow](https://user-images.githubusercontent.com/93447311/169734084-5509824b-5cfa-4740-a716-8d6bddd8fbf2.png)

how to install quetzal: `pip install quetzal`

quick start with quetzal:
```py
import quetzal as qtz

db = qtz.connection("database")
db.execute("CREATE TABLE users (id INTEGER NOT NULL AUTO INCREMENT UNIQUE DEFAULT 1, user TEXT NOT NULL UNIQUE)")
db.execute("INSERT INTO users VALUES(1,'user')")
db.commit()
```
