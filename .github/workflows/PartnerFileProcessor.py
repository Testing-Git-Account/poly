import pandas


class PartnerFileProcessor():
    def __init__(self, filepath):
        self.filepath = filepath

    def data_extractor(self):
        return ""


if __name__ == "__main__":
    file_path = ''
    processor_obj = PartnerFileProcessor(file_path)
    sum = processor_obj.data_extractor()

