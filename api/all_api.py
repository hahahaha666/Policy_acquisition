from sanic import Blueprint
from sanic import response, Sanic
from sanic_validation import validate_json
from utils import log
from utils.common import  global_catch_exception
from worker.政策栏目页_测试 import  *
logger = log.write_log(work_name='api_info')

"""
加跨域问题 先测试
"""

verify_handler = Blueprint('zc_api', url_prefix='/api')
schema = {
    'id': {'type': 'string', 'required': True},##栏目也id
}

schema_2 = {
    'cid': {'type': 'string', 'required': True},
    'url': {'type': 'string', 'required': True},
    'post_data': {'type': 'string', 'required': True},

}


@global_catch_exception
@verify_handler.route("/zccolumn", methods=["POST",'OPTIONS'])
@validate_json(schema)
async def handle_proxy_request(request):
    """
    政策采集栏目页
    """
    resp = {"msg": "success", "data": "", "code": 0}
    form_data = request.json
    id = form_data["id"]  # `dict`
    verify_result = await ZC_getcolumn().sql(id)
    resp["verify_result"] = verify_result
    if verify_result==None:
        resp['code']=103
    return response.json(resp)


@global_catch_exception
@verify_handler.route("/hycolumn", methods=["POST",'OPTIONS'])
@validate_json(schema)
async def handle_proxy_request(request):
    """
    行业采集 栏目页
    """
    resp = {"msg": "success", "data": "", "code": 0}
    form_data = request.json
    id = form_data["id"]  # `dict`
    browser = form_data['browser']
    verify_result = await Get_x5sec().get_x5(url)
    resp["verify_result"] = verify_result
    if verify_result==None:
        resp['code']=103
    return response.json(resp)

@global_catch_exception
@verify_handler.route("/zcdetails", methods=["POST",'OPTIONS'])
@validate_json(schema)
async def handle_proxy_request(request):
    """
    政策采集 详情页
    """
    resp = {"msg": "success", "data": "", "code": 0}
    form_data = request.json
    id = form_data["id"]  # `dict`
    browser = form_data['browser']
    verify_result = await Get_x5sec().get_x5(url)
    resp["verify_result"] = verify_result
    if verify_result==None:
        resp['code']=103

@global_catch_exception
@verify_handler.route("/hydetails", methods=["POST",'OPTIONS'])
@validate_json(schema)
async def handle_proxy_request(request):
    """
    行业采集 详情页
    """
    resp = {"msg": "success", "data": "", "code": 0}
    form_data = request.json
    id = form_data["id"]  # `dict`
    browser = form_data['browser']
    verify_result = await Get_x5sec().get_x5(url)
    resp["verify_result"] = verify_result
    if verify_result==None:
        resp['code']=103


if __name__ == '__main__':
    app = Sanic(__name__)
    app.blueprint(verify_handler)
    app.run(host='0.0.0.0', port=5001, debug=True)
