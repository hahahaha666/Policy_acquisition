from flask import Flask, g
from flask_restful import reqparse, Api, Resource
from gevent import monkey
from gevent.pywsgi import WSGIServer
from flask_cors import CORS
from geventwebsocket.handler import WebSocketHandler
# WebSocketHandler.path_all()
from sd_insertsql import *
from flask import Flask
from flask import request
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
        if request.method == "GET" or request.method=="OPTIONS" :
            sid=request.args.get("sid")
            industry_id=request.args.get("industry_id")
            if sid:
                print(sid)
                sid=sid.split(",")
                type="zc"
                return main(sid,type)
            elif industry_id:
                industry_id = industry_id.split(",")
                print(industry_id)
                type="hy"
                return main(industry_id,type)
api.add_resource(HelloWorld, '/')
# 设置路由，即路由地址为http://127.0.0.1:5000/users


if __name__ == "__main__":
    #app.run(debug=
    CORS(app, supports_credentials=True)
    app.run(host='0.0.0.0', port=5003,debug=True,threaded=True)
    http_server = WSGIServer(('0.0.0.0', 5003), app, handler_class=WebSocketHandler)
    http_server.serve_forever()