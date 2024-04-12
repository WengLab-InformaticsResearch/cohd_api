from bmt import Toolkit


# Static instance of the Biolink Model Toolkit
bm_version = 'v4.1.6'
bm_toolkit = Toolkit(f'https://raw.githubusercontent.com/biolink/biolink-model/{bm_version}/biolink-model.yaml')