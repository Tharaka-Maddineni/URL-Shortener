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

db_object = PostGreSQL(host, port, dbname, user, password)   # Creating PostGreSQL database object
db_object.createTable()

def shorten_Url(url):                                        # Function to shorten the URL

    shorten_url = db_object.generate_short_link()
    db_object.add_long_short_to_DB(url, shorten_url)

    return shorten_url

@app.route('/', methods = ['POST','GET'])
def get_short_URL():
    if request.method == 'POST':        
        original_url = request.form['url']                            # Original url from form        
        search = request.form['search_id']                            # Search term from form

        if search is not None:
            original_url = str(original_url) + "/search?q=" + str(search)

        found_url = db_object.check_if_url_exists(original_url)       # Check if URL already exists in DB
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


@app.route('/<short_url>')
def redirect_to_url(short_url):
    link = db_object.get_original_link_from_DB(short_url)

    db_object.add_up_url_visits(short_url)         # Add up the url visits by 1 when ever this redirect invokes

    return redirect(link)


if __name__ == "__main__":
    app.run(debug=True)