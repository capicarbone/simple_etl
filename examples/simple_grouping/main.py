import sys

sys.path.insert(0, "../../../simple_etl")

from typing import Annotated
from simple_etl import GroupedETLTask
from simple_etl.locators import Column
from simple_etl.readers import CSVReader
from simple_etl.loaders import RichConsole

task = GroupedETLTask()


@task.group()
def group_by_product(product_name: Annotated[str, Column("Product")]):
    return product_name.lower().strip()


@task.computed("product")
def product(product_name: Annotated[str, Column("Product")], _=None):
    return product_name


@task.computed("total")
def total(
    qty: Annotated[str, Column("Qty")], price: Annotated[str, Column("Price")], agg=0
):
    agg = 0 if not agg else agg
    return agg + int(qty) * float(price)


if __name__ == "__main__":
    task.load(CSVReader("sales_data.csv"), RichConsole())

