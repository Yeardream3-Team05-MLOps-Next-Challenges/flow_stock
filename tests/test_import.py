import pytest
import importlib

def _check_import(module_name):
    try:
      importlib.import_module(module_name)
      return None
    except ImportError as e:
       return f"{module_name}: {e}"

def test_library_imports():
   modules = ["httpx", "kafka", "requests", "prefect", "websockets", "pytz"]
   failed_imports = []
   for module in modules:
      error_message = _check_import(module)
      if error_message:
         failed_imports.append(error_message)

   if failed_imports:
      pytest.fail(f"Failed to import required libraries:\n{', '.join(failed_imports)}")