from datetime import datetime

def generate_batch_id(pipeline_name: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{pipeline_name}_{timestamp}"