from flask import Flask, g
from flask_restful import reqparse, Api, Resource
from gevent import monkey
monkey.patch_all()
from gevent.pywsgi import WSGIServer
import  json
from flask_cors import CORS
import ast
from geventwebsocket.handler import WebSocketHandler
# WebSocketHandler.path_all()
from flask import Flask
from flask import request
# from 政策爬虫_new.test_xq import *
from Policy_acquisition.test_details import *
app = Flask(__name__)
CORS(app,resources=r'/*')
api = Api(app)
app.config.update(
    DEBUG=True
)
#异步处理相关操作
monkey.patch_all()
@app.route("/HelloWorld/", methods=["GET", "POST","OPTIONS"])
class HelloWorld(Resource):
    def get(self):
        if request.method == "GET"  :
            cid=request.args.get("cid")
            url=request.args.get("url")
            post_data = request.args.get("post_data")
            post_data=json.loads(post_data)
            industry_cid=request.args.get("industry_cid")
            if cid:
                c="zc"
                return main_i(cid, url,c,post_data)
            elif industry_cid:
                c="hy"
                return main_i(industry_cid, url,c,post_data)
api.add_resource(HelloWorld, '/')
# 设置路由，即路由地址为http://127.0.0.1:5000/users


if __name__ == "__main__":
    #app.run(debug=
    CORS(app, supports_credentials=True)
    app.run(host='0.0.0.0', port=5002,debug=True,threaded=True)
    http_server = WSGIServer(('0.0.0.0', 5002), app, handler_class=WebSocketHandler)
    http_server.serve_forever()