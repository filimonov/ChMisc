
import logging
import xml.etree.ElementTree as ET
import csv


class XMLConfigToCSVConverter:
    """
    Class to convert XML config to CSV file (xpath, attributes, value)
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def __strip_heading_and_trailing_whitespace(text):
        if text is None:
            return None
        return text.strip()

    def __print_xpath_csv(self, node, writer, version, tag_count=None, parent_xpath=''):
        if tag_count is None:
            tag_count = {}

        # Create the current node's XPath by appending its tag to the parent XPath
        tag = node.tag
        xpath = parent_xpath + '/' + tag
        tag_count[tag] = tag_count.get(tag, 0) + 1

        # Add a sequence number to the XPath if the tag occurs multiple times
        if tag_count[tag] > 1:
            xpath += f"[{tag_count[tag]}]"

        # Create a list containing the current node's XPath, attributes, and values
        values = [version, xpath, node.attrib, self.__strip_heading_and_trailing_whitespace(node.text)]

        # Write the values to the CSV file using the provided writer object
        writer.writerow(values)

        # Recursively call this function for each of the current node's children
        tag_count_cp = tag_count.copy()
        for child in node:
            self.__print_xpath_csv(child, writer, version, tag_count_cp, xpath)

    def __get_tsv_writer(self, stream):
        writer = csv.writer(stream, delimiter='\t', escapechar='\\', quoting=csv.QUOTE_NONE, lineterminator='\n')
        writer.writerow(['version', 'xpath', 'attributes', 'value'])
        writer.writerow(['LowCardinality(String)', 'String', 'Map(String, String)', 'String'])
        return writer

    def dump_xml(self, version, xml_raw, file_name):
        try:
            xml_str = ET.canonicalize(xml_raw)
            # Parse the XML string into an ElementTree object
            root = ET.fromstring(xml_str)

            if file_name == '-':
                stream = sys.stdout
            else:
                stream = open(file_name, 'w')

            with stream:
                writer = self.__get_tsv_writer(stream)
                self.__print_xpath_csv(root, writer, version)

        except ET.ParseError as e:
            self.logger.error(f"XML parsing error: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error processing XML: {e}")
            raise
