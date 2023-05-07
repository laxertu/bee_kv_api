from fastapi import FastAPI

import bee_kv.public.driver.ports as ports
from bee_kv.public.driver.factory import get_handler, get_request
from bee_kv.public.driver.entities import KvDataOperationRequest, KvDataOperationResponse, KvDto

app = FastAPI()


class Adapter(ports.Port):

    def validate(self, request: KvDataOperationRequest):
        pass

    def handle(self, request: KvDataOperationRequest) -> KvDataOperationResponse:
        handler = get_handler(request.cmd)
        response = handler.handle(request)
        return KvDataOperationResponse(response.payload)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/ws/kv/")
@app.get("/ws/kv/get_all/")
async def kv_ep():
    handler = get_handler(cmd=ports.CMD_GET_ALL)
    request = get_request(cmd=ports.CMD_GET_ALL)

    result = handler.handle(request)
    return {kv.payload.key: kv.payload.value for kv in result}


@app.get("/ws/kv/get/{key}")
async def kv_ep(key: str):
    handler = get_handler(cmd=ports.CMD_GET)
    request = get_request(cmd=ports.CMD_GET)

    request.payload.key = key

    result = handler.handle(request)
    return result.payload.value


@app.post("/ws/kv/save/")
async def kv_ep(kv: KvDto):
    handler = get_handler(cmd=ports.CMD_SAVE)
    request = get_request(cmd=ports.CMD_SAVE)

    request.payload = kv
    return handler.handle(request).payload


@app.post("/ws/kv/remove/{key}")
async def kv_ep(key: str):
    handler = get_handler(cmd=ports.CMD_REMOVE)
    request = get_request(cmd=ports.CMD_REMOVE)

    request.payload.key = key
    handler.handle(request)
    return None


@app.post("/ws/kv/reset")
async def kv_ep():
    handler = get_handler(cmd=ports.CMD_RESET)
    request = get_request(cmd=ports.CMD_RESET)

    handler.handle(request)
    return None
