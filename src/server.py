#!/usr/bin/env python
# encoding: utf-8
import json
import time
from flask import Flask
app = Flask(__name__)
@app.route('/')
def index():
    time.sleep(1)
    return json.dumps({'name': 'alice',
                       'email': 'alice@outlook.com'})
if __name__ == '__main__':
    app.run(host='127.0.0.1', port=3000)
