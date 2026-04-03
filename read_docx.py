import zipfile
import xml.etree.ElementTree as ET
import sys
import os

def read_docx(file_path):
    try:
        with zipfile.ZipFile(file_path) as docx:
            xml_content = docx.read('word/document.xml')
            tree = ET.fromstring(xml_content)
            
            ns = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
            
            paragraphs = []
            for p in tree.findall('.//w:p', ns):
                texts = [node.text for node in p.findall('.//w:t', ns) if node.text]
                if texts:
                    paragraphs.append(''.join(texts))
            return '\n'.join(paragraphs)
    except Exception as e:
        return f"Error reading {file_path}: {e}"

if __name__ == "__main__":
    with open("parsed_docs.md", "w", encoding="utf-8") as out:
        if len(sys.argv) > 1:
            for file in sys.argv[1:]:
                out.write(f"# {os.path.basename(file)}\n")
                out.write(read_docx(file))
                out.write("\n\n")
