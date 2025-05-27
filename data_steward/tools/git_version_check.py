from git import Repo
import os

def get_git_tag():
    """
    gets latest git tag.
    :return: git tag in string format
    """
    repo = Repo(os.getcwd(), search_parent_directories=True)
    try:
        tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime)
        # tag_ref = TagReference.list_items(repo)[-1].name
        tag_ref = tags[-1]
    except IndexError:
        tag_ref = ''
    print(tag_ref)

if __name__ == '__main__':
    get_git_tag()