from distutils.core import setup

setup(
    # How you named your package folder (MyLib)
    name='rabbitmq_pika_flask',
    packages=['rabbitmq_pika_flask'],   # Chose the same as "name"
    # Start with a small number and increase it with every change you make
    version='0.0.20',
    # Chose a license from here: https://help.github.com/articles/licensing-a-repository
    license='MIT',
    # Give a short description about your library
    description='Adapter for RabbitMQs pika and flask',
    author='Aylton Almeida',                   # Type in your name
    author_email='almeida@aylton.dev',      # Type in your E-Mail
    # Provide either the link to your github or to your website
    url='https://github.com/aylton-almeida/rabbitmq-pika-flask',
    # I explain this later on
    download_url='https://github.com/aylton-almeida/rabbitmq-pika-flask/archive/0.1.tar.gz',
    # Keywords that define your package best
    keywords=['FLASK', 'PIKA', 'RABBITMQ', 'MESSAGEQUEUE'],
    install_requires=[            # I get to this in a second
        'Flask>=1.1.1',
        'pika>=1.2.0',
    ],
    classifiers=[
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Development Status :: 3 - Alpha',
        # Define that your audience are developers
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',   # Again, pick a license
        # Specify which pyhton versions that you want to support
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.8',
    ],
)
