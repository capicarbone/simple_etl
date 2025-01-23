
Simple ETL
---

Simple ETL is a library for Python intended for writing ETL scripts with focus on testability and readability, hence, code easier to maintain, with an eye on optimization.

```python
from typing import Annotated
from simple_etl import ETLTask, CSVReader, CSVLoader, Column

task = ETLTask()

# Copying values from the input but with proper names
task.passthrough(Column("product_name"), "Product Name")
task.passthrough(Column("qty"), "Quantity")

# Formatting price.
# We need the price column found in the input file, so it's defined 
# as a function parameter. 
@task.computed('Price')
def price(
        price: Annotated[float, Column('price')]
):
    return f"${price}"

# Calculating the total for each product in a shopping cart. 
# We define as dependencies for this one the quantity
# and the price columns
@task.computed("Total")
def product_total(
        qty: Annotated[str, Column('qty')],
        price: Annotated[str, Column('price')]
):
    return f"${round(int(qty) * float(price), 2)}"

# Setting up the output and the input
with open("shoping_cart.csv") as csvfile:
    reader = CSVReader(csvfile)
    loader = CSVLoader("invoice.csv")

    # Executing the ETL task.
    task.load(reader, loader)
    
# Formatted output example in invoice.csv

# | Product Name | Quantity | Price | Total |
# |--------------|----------|-------|-------|
# | Apple        | 3        | $0.5  | $1.5  |
# | Mango        | 5        | $1    | $5    |
# | Dragonfruit  | 2        | $0.7  | $1.4  |

```
For computed outputs, the ETLTask object will provide the required values, and it will execute the defined function for each row in the input file. From this code, you can target your unit tests to the computed functions and/or test the ETLTask with more convenient readers and loaders before going to production.

Each input row is processed and its output loaded as they are read, thus keeping memory usage as low as possible.

### Work in progress