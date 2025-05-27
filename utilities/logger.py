import logging

def get_logger(name=__name__):
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    return logging.getLogger(name)

if __name__ == "__main__":
    get_logger()