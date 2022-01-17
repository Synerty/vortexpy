import json
import os.path

from vortex.Tuple import Tuple
from vortex.Tuple import TupleField
from vortex.Tuple import addTupleType

JSON_DIR = os.path.expanduser("~/my-app-storage.json")


@addTupleType
class MyItem(Tuple):
    __tupleType__ = "MyItem"
    myBool: bool = TupleField()
    myStr: str = TupleField()


@addTupleType
class MyData(Tuple):
    __tupleType__ = "MyData"
    myItems: list[MyItem] = TupleField([])
    myName: str = TupleField()


def store(tuple_: Tuple):
    jsonObj = tuple_.tupleToRestfulJsonDict()
    jsonStr = json.dumps(
        jsonObj,
        indent=4,
        sort_keys=True,
        separators=(", ", ": "),
    )

    with open(JSON_DIR, "wb") as f:
        f.write(jsonStr.encode())
        f.write(b"\n")


def load() -> MyData:
    with open(JSON_DIR, "rb") as f:
        jsonStr = f.read().decode()

    jsonObj = json.loads(jsonStr)

    tuple_ = Tuple.restfulJsonDictToTupleWithValidation(jsonObj, MyData)
    assert isinstance(tuple_, MyData), "tuple_ is not of type MyData"
    return tuple_


if __name__ == "__main__":
    # Make the data
    myData = MyData(myName="A good name")
    myData.myItems.append(MyItem(myBool=False, myStr="store me"))
    myData.myItems.append(MyItem(myBool=True, myStr="and me too"))

    store(myData)

    myLoadedData = load()

    assert myData.myName == myLoadedData.myName, "myName didn't load correctly"
