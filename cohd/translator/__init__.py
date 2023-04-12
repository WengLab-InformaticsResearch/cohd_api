from bmt import Toolkit


# Static instance of the Biolink Model Toolkit
bm_version = 'v3.2.7'
bm_toolkit = Toolkit(f'https://raw.githubusercontent.com/biolink/biolink-model/{bm_version}/biolink-model.yaml')