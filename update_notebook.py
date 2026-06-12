import json

notebook_path = "model/mortality_model/xai_shap_experiment_what_if.ipynb"

with open(notebook_path, 'r', encoding='utf-8') as f:
    nb = json.load(f)

for cell in nb['cells']:
    if cell['cell_type'] == 'code':
        new_source = []
        for line in cell['source']:
            # Replace old data path
            if "base_path =" in line and "kaggle" in line:
                line = 'base_path = "/kaggle/input/datasets/anhkhang/bich-data/analytical_dataset_with_notes_2"\n'
            
            # Replace model path 
            if "imputer_path =" in line:
                line = '        imputer_path = "/kaggle/input/datasets/tdduat/mortality-models/mortality_models/fitted_imputer.joblib"\n'
            
            new_source.append(line)
        cell['source'] = new_source

with open(notebook_path, 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=1)

print("Updated paths in notebook successfully.")
