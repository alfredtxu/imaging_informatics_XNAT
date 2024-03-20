import os
import shutil
import argparse
import xnat


def create_project(args, project):
    # To be implemented
    print('Sorry, to be implemented.')


def create_folders(args):

    xnat_data = os.path.join(args.xnatdatapath, 'xnat-data')
    archive = os.path.join(xnat_data, 'archive')
    build = os.path.join(xnat_data, 'build')
    cache = os.path.join(xnat_data, 'cache')
    home = os.path.join(xnat_data, 'home')
    logs = os.path.join(home, 'logs')

    postgres_data = os.path.join(args.xnatdatapath, 'postgres-data')

    if not os.path.exists(xnat_data):
        os.makedirs(xnat_data)
    if not os.path.exists(archive):
        os.makedirs(archive)
    if not os.path.exists(build):
        os.makedirs(build)
    if not os.path.exists(cache):
        os.makedirs(cache)
    if not os.path.exists(home):
        os.makedirs(home)
    if not os.path.exists(logs):
        os.makedirs(logs)

    if not os.path.exists(postgres_data):
        os.makedirs(postgres_data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create Xnat folders')
    parser.add_argument('-xp', '--xnatdatapath', type=str, default='/home/samia/Documents/xnatuser/XNAT-DATA',
                        help='path to store xnat data')
    parser.add_argument('-cf', '--createfolders', type=bool, default=False,
                        help='create xnat data folders if they do not exist')
    parser.add_argument('-cp', '--createproject', nargs=2, help="Pass True/False as first argument and projectName"
                        " as second argument to create a project")

    args = parser.parse_args()
    if args.createfolders:
        create_folders(args)
    if args.createproject[0]:
        create_project(args, args.createproject[1])

