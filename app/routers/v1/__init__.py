"""
This module imports the `endpoints` submodule from the same directory (app/routers/v1/).

The `endpoints` submodule contains the API endpoints and routes for version 1 of the application.
By importing it here, the routes defined in `endpoints` become available for use in other parts of the application.
"""
from . import endpoints
