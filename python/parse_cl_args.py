import sys

def parser():
    if len(sys.argv) > 1:
        mystr = sys.argv[1]
        for item in mystr.split():
            print(f"{item}")

if __name__ == "__main__":
    parser()