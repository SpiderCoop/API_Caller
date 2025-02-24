from setuptools import setup

setup(
    name="Banxico_api",  # Nombre del paquete (debe coincidir con la carpeta del módulo)
    version="0.1.0",  # Versión inicial del paquete
    author="David Jiménez Cooper",
    author_email="david.jimenez.cooper@gmail.com",
    description="Un paquete con funciones diseñadas para facilitar la conexión con la API del SIstema de Información Económica de Banco de México",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/SpiderCoop/API_Caller/tree/main/Banxico_api",  # URL de tu repositorio en GitHub
    packages=['Banxico_api'],  
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "pandas",
        "requests",
        "python-dotenv",
    ],
    python_requires=">=3.6",  # Versión mínima de Python compatible
)
