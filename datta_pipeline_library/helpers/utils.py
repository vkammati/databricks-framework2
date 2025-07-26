"""
Helpers for Helper functions
"""
VALID_ENVS = ["dev", "tst", "pre", "prd"]
VALID_LAYERS = ["landing", "raw", "enriched-unharmonized", "enriched-harmonized", "curated"]


def assert_env(env: str):
    """
    Assert Env
    @return: Boolean
    """
    assert env in VALID_ENVS, f"Environment must be one of {VALID_ENVS}, instead got {env}."

    
def assert_layer(layer: str):
    """
    Assert Layer
    @return: Boolean
    """
    assert layer in VALID_LAYERS, f"Layer must be one of {VALID_LAYERS}, instead got {layer}."
