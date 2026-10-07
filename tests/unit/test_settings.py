import pytest
import yaml

from data_engineering_pyspark.config.settings import carregar_config


@pytest.fixture
def arquivo_config_valido(tmp_path):
    config_data = {
        "spark": {"app_name": "TestApp"},
        "paths": {"clientes": "/d/c.json.gz", "pedidos": "/d/p/", "output": "/d/out/"},
        "file_options": {"pedidos_csv": {"compression": "gzip", "header": True, "sep": ";"}},
    }
    config_file = tmp_path / "settings.yaml"
    config_file.write_text(yaml.dump(config_data))
    return str(config_file)


class TestCarregarConfig:

    def test_le_valores_do_yaml(self, arquivo_config_valido):
        resultado = carregar_config(arquivo_config_valido)
        assert resultado["spark"]["app_name"] == "TestApp"
        assert resultado["file_options"]["pedidos_csv"]["sep"] == ";"

    def test_arquivo_inexistente_lanca_excecao(self):
        """Melhor falhar rápido e claro do que seguir com None e quebrar só mais adiante."""
        with pytest.raises(FileNotFoundError):
            carregar_config("/caminho/inexistente/settings.yaml")
