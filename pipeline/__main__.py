import os
import sys
from pipeline import main

if __name__ == "__main__":
    current_working_dir_path = os.path.join(os.path.dirname(sys.argv[0]), os.path.pardir)

    main(current_working_dir_path=current_working_dir_path)