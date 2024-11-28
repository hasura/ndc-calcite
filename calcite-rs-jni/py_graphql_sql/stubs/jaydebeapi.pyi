from typing import Any, List, Optional, Union

from java.util import Properties

def connect(
    jclassname: str,
    url: str,
    driver_args: Optional[List[Union[str, Properties]]] = None,
    jars: Optional[Union[str, List[str]]] = None,
    libs: Optional[Union[str, List[str]]] = None,
) -> Any: ...
