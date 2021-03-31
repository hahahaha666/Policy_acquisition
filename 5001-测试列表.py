from flask import Flask, g
from flask_restful import reqparse, Api, Resource
from gevent import monkey
monkey.patch_all()
from gevent.pywsgi import WSGIServer
from flask_cors import CORS
from geventwebsocket.handler import WebSocketHandler
from flask import Flask
from Policy_acquisition.test_column  import *
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
        if request.method == "GET" :
            id = request.args.get("id")
            browser=request.args.get("browser")
            industry_id=request.args.get("industry_id")
            if id:
                browser="zc"
                return main(id, browser)
            elif industry_id:
                browser="hy"
                return main(industry_id,browser)
api.add_resource(HelloWorld, '/')
# 设置路由，即路由地址为http://127.0.0.1:5000/users


if __name__ == "__main__":
    #app.run(debug=
    CORS(app, supports_credentials=True)
    app.run(host='0.0.0.0', port=5001,debug=True,threaded=True)
    http_server = WSGIServer(('0.0.0.0', 5001), app, handler_class=WebSocketHandler)
    http_server.serve_forever()