import argparse
def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=str, help='Provide a prompt to split')
    args = parser.parse_args()
    for word in args.input.split():
        print(word)
if __name__ == "__main__":
   parser()
