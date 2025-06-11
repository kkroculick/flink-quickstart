import os
import sys

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import row, col

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')])
table.execute().print()
