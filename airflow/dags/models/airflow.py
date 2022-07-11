from airflow.models.variable import Variable
from typing import Any


def environment_resolver(if_prod: Any, if_lower: Any) -> Any:
    """
    Resolves Airflow environment and executes/returns parameter accordingly.
    :param if_prod: If value is callable it must be passed as a lambda.
            Eg: ``lambda: print('Value')``
        Other types are returned.
    :type if_prod: Any
    :param if_lower: If value is callable it must be passed as a lambda.
            Eg: ``lambda: print('Value')``
        Other types are returned.
    :type if_lower: Any
    """
    ENVIRONMENT = Variable.get("environment")

    if ENVIRONMENT == "prod":
        if callable(if_prod):
            if_prod()
        else:
            return if_prod
    elif ENVIRONMENT in [
        "dev",
    ]:
        if callable(if_lower):
            if_lower()
        else:
            return if_lower
    else:
        raise ValueError(f"Unknown Environment: {ENVIRONMENT}")
