from flask import Flask, render_template, redirect, url_for, request
from collections import defaultdict as ddict

app = Flask(__name__)

ban_dict = ddict(lambda: False)
# use decorators to link the function to a url
@app.route('/')
def home():
    return "Hello, World!"  # return a string


@app.route('/welcome')
def welcome():
    return render_template('welcome.html')  # render a template


def val_login_page(request):
    if request.form['username'] != 'admin' or request.form['password'] != 'admin':
        error = 'Invalid Credentials. Please try again.'
    else:
        return render_template('manager.html')

def check_ban(worker_id):
    return ban_dict[worker_id]

def ban_worker(worker_id):
    ban_dict[worker_id] = True

def unban_worker(worker_id):
    ban_dict[worker_id] = False

def get_banned_workers():
    banned = []
    for k, v in ban_dict.iteritems():
        if v:
            banned.append(k)
    return banned

@app.route('/manager', methods=['GET', 'POST'])
def manager():
    error = None
    if request.method != 'POST':
        # first time on the page
        return render_template('manager.html', error=error).replace('REPLACE_ME',info_string)
    if request.form['username'] != 'admin' or request.form['password'] != 'admin':
        error = 'Invalid Credentials.'
        return render_template('manager.html', error=error).replace('REPLACE_ME',info_string)
    info_string = ''
    to_check = [x.strip().upper() for x in request.form['check_ban'].split(',')]
    to_check = filter(lambda x: len(x), to_check)
    to_ban = [x.strip().upper() for x in request.form['ban_worker'].split(',')]
    to_ban = filter(lambda x: len(x), to_ban)
    to_unban = [x.strip().upper() for x in request.form['unban_worker'].split(',')]
    to_unban = filter(lambda x: len(x), to_unban)
    is_banned = [check_ban(x) for x in to_check]
    _ = [ban_worker(x) for x in to_ban]
    _ = [unban_worker(x) for x in to_unban]
    if len(to_check):
        info_string += '<b>TO CHECK</b><br>'
        for tochk, chkv in zip(to_check, is_banned):
            info_string += tochk + ': ' + str(chkv) + '<br>'
        info_string += '<br>'
    if len(to_ban):
        info_string += '<b>TO BAN</b><br>'
        info_string += '<br>'.join(to_ban)
        info_string += '<br><br>'
    if len(to_unban):
        info_string += '<b>TO UNBAN</b><br>'
        info_string += '<br>'.join(to_unban)
        info_string += '<br><br>'
    if request.form['submit'] == 'Submit':
        return render_template('manager.html').replace('REPLACE_ME',info_string)
    elif request.form['submit'] == 'List Banned':
        is_banned = get_banned_workers()
        info_string += 'BANNED WORKERS<br>'
        info_string += '<br>'.join(is_banned) + '<br><br>'
    elif request.form['submit'] == 'Stop':
        info_string += 'Stopping<br>'
    elif request.form['submit'] == 'Halt':
        info_string += 'Halting<br>'
    elif request.form['submit'] == 'Shutdown':
        info_string += 'Shutting down<br>'
    return render_template('manager.html').replace('REPLACE_ME',info_string)

if __name__ == '__main__':
    app.run(debug=True)
