from typing import Any, Optional, Union, overload

def startJVM(jvmpath: Optional[str] = None, *args: str, **kwargs: Any) -> None: ...
def getDefaultJVMPath() -> str: ...
def isJVMStarted() -> bool: ...
def JClass(name: str) -> Any: ...
