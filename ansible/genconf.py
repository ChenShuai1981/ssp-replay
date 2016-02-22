# coding=utf-8

import os
import sys
import yaml
import jinja2
from jinja2 import Environment,PackageLoader

_workdir=os.getcwd()
_templatedir=os.path.join(_workdir,'templates')

def getTemplateFile():
    templateFiles=os.listdir(_templatedir);
    return templateFiles

def renderFile(template,d):
    env = Environment(loader=PackageLoader(sys.argv[0][:-3],'templates'),undefined=jinja2.StrictUndefined)
    template = env.get_template(template)
    return template.render(**d) + '\n'

def writeConf(d,c,n):
    if not os.path.exists(d):
        os.makedirs(d)
    fn = os.path.join(d, n)
    with open(fn, 'w') as f:
        f.write(c)
    os.chmod(fn, 0o755)

def main():
    targetEnv=sys.argv[1] if len(sys.argv)>1 else 'local'
    targetDir=os.path.join(_workdir,'target',targetEnv)
    confDir=os.path.join(_workdir,'src')
    confFile=confDir+'/'+targetEnv+'.yaml'
    with open(confFile,'r') as f:
    	_renderVar=yaml.load(f)
    tf=getTemplateFile()
    for file in tf:
        writeConf(targetDir,renderFile(file,_renderVar[file]),file)

if __name__ == '__main__':

        main()
        print('\ngenerated configuration files success!\n')

        os._exit(1)
