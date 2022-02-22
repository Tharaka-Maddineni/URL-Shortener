import pyshorteners
from flask import Flask,render_template,request,redirect
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
#db_object.createTable()

# Function to shorten the URL
def shorten_Url(url):

    shorten_url = pyshorteners.Shortener().tinyurl.short(url)

    # Upload or insert long url and short url into database
    db_object.add_long_short_to_DB(url, shorten_url)
    return shorten_url


@app.route('/', methods = ['POST','GET'])
def get_short_URL():
    if request.method == 'POST':
        original_url = request.form['url']

        # Check if URL already exists in DB
        found_url = db_object.check_if_url_exists(original_url)
        if found_url:
            shorten_url = db_object.get_shorten_url_from_DB(original_url)
            return render_template('index.html',
                                   short_url=shorten_url)
        else:
            shorten_url = shorten_Url(original_url)
            return render_template('index.html',
                               short_url = shorten_url)
    else:
        return render_template('index.html')

if __name__ == "__main__":
    app.run(debug=True)