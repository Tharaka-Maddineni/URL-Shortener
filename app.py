from pyshorteners import Shortener
from flask import Flask,render_template,request
from DBOperations import PostGreSQL

app = Flask(__name__)

# DataBase connectivity parameters
host = '127.0.0.1'
port = '5432'
dbname = 'url'
table_name = 'url'
user = 'postgres'
password = 'Walker1510'

# Creating PostGreSQL database object
db_object = PostGreSQL(host, port, dbname, user, password)
db_object.createTable()

@app.route('/', methods = ['POST','GET'])
def get_short_URL():
    if request.method == 'POST':
        long_url = request.form['url']
        # custom_id = request.form['custom_id']

        # Check if URL already exists in DB
        found_url = db_object.check_if_url_exists(long_url)
        if found_url:
            shorten_url = "select short from url where long = {}".format(long_url)
        else:
            pass
        return render_template('results.html', shorten_url = shorten_url)
    else:
        return render_template('index.html')

if __name__ == "__main__":
    app.run(debug=True)