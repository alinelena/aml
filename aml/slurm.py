#!/usr/bin/env python3 

import os 
import subprocess 
import time
import shlex, shutil
import configparser

class sbatch:
	def __init__(self,account = None, constraint = None, nodes = None, mpis_per_node = None,
                     cores_per_mpi = 1, memory = None, reservation = None, name = 'noname',cmd='hostname',
                     time = '00:01:00',workdir='./',jobfile='a.slurm',pre='#preamble'):
		self.__set_jobid(None)
		self.__set_name(name)
		self.__set_nodes(nodes)
		self.__set_returncode(None)
		self.__set_cmd(cmd)
		self.__set_time(time)
		self.__set_pre(pre)
		self.__set_workdir(os.path.abspath(workdir))
		self.__set_jobfile(jobfile)
		self.__set_memory(memory)
		self.__set_account(account)
		self.__set_reservation(reservation)
		self.__set_constraint(constraint)
		self.__set_mpis_per_node(mpis_per_node)
		self.__set_cores_per_mpi(cores_per_mpi)
		with open(os.path.join(workdir,jobfile),'w') as f:
			print(self,file=f)

	def __get_account(self):
		return self.__account
	
	def __set_account(self,x):
		self.__account = x
	
	account = property(__get_account, __set_account)
	
	def __get_reservation(self):
		return self.__reservation
	
	def __set_reservation(self,x):
		self.__reservation = x
	
	reservation = property(__get_reservation, __set_reservation)
	
	def __get_constraint(self):
		return self.__constraint
	
	def __set_constraint(self,x):
		self.__constraint = x
	
	constraint = property(__get_constraint, __set_constraint)
	
	def __get_mpis_per_node(self):
		return self.__mpis_per_node
	
	def __set_mpis_per_node(self,x):
		self.__mpis_per_node = x
	
	mpis_per_node = property(__get_mpis_per_node, __set_mpis_per_node)
	
	def __get_cores_per_mpi(self):
		return self.__cores_per_mpi
	
	def __set_cores_per_mpi(self,x):
		self.__cores_per_mpi = x
	
	cores_per_mpi = property(__get_cores_per_mpi, __set_cores_per_mpi)
	
	def __get_memory(self):
		return self.__memory
	
	def __set_memory(self,x):
		self.__memory = x
	
	memory = property(__get_memory, __set_memory)
	
	def __get_cmd(self):
		return self.__cmd
	
	def __set_cmd(self,x):
		self.__cmd = x
	
	cmd = property(__get_cmd, __set_cmd)
	
	def __get_time(self):
		return self.__time
	
	def __set_time(self,x):
		self.__time = x
	
	time = property(__get_time, __set_time)
	
	def __get_pre(self):
		return self.__pre
	
	def __set_pre(self,x):
		self.__pre = x
	
	pre = property(__get_pre, __set_pre)
	
	def __get_workdir(self):
		return self.__workdir
	
	def __set_workdir(self,x):
		self.__workdir = x
	
	workdir = property(__get_workdir, __set_workdir)
	
	def __get_jobfile(self):
		return self.__jobfile
	
	def __set_jobfile(self,x):
		self.__jobfile = x
	
	jobfile = property(__get_jobfile, __set_jobfile)
	
	def __get_jobid(self):
		return self.__jobid
	
	def __set_jobid(self,x):
		self.__jobid = x
	
	jobid = property(__get_jobid, __set_jobid)
	
	def __get_returncode(self):
		return self.__returncode
	
	def __set_returncode(self,x):
		self.__returncode = x
	
	returncode = property(__get_returncode, __set_returncode)
	
	
	def __get_name(self):
		return self.__name

	def __set_name(self,x):
		self.__name = x
	
	name = property(__get_name, __set_name)

	def __get_nodes(self):
		return self.__nodes
	
	def __set_nodes(self,x):
		self.__nodes = x
	
	nodes = property(__get_nodes, __set_nodes)

	def __str__(self):
		a ='#!/usr/bin/env bash'
		a += '\n#SBATCH --job-name="{}"'.format(self.name)
		a += '\n#SBATCH --exclusive'
		a += "\n#SBATCH --nodes={}".format(self.nodes)
		a += '\n#SBATCH -t {}'.format(self.time)
		if self.constraint is not None:
			a += '\n#SBATCH -C "{}"'.format(self.constraint)
		if self.account is not None:
			a += "\n#SBATCH --account={}".format(self.account)
		if self.reservation is not None:
			a += "\n#SBATCH --reservation={}".format(self.reservation)
		if self.mpis_per_node is not None:
			a += "\n#SBATCH --tasks-per-node={}".format(self.mpis_per_node)
		a += "\n#SBATCH --cpus-per-task={}".format(self.cores_per_mpi)
		if self.memory is not None:
			a += "\n#SBATCH --mem-per-cpu={}".format(self.memory)
                
		a += '\n\n{}'.format(self.pre)
		a += '\n\n{}'.format(self.cmd)
		return a
	
	def run(self):
		a = shlex.split(shutil.which('sbatch') + ' ' + self.jobfile)
		x = subprocess.run(a, cwd=self.workdir, shell=False,capture_output=True,text=True)
		try:
			z = x.stdout.split()
			self.jobid = int(z[3])
		except IndexError:
			print('error submittine the job')
			print('coomand was {}'.format(x.args))
			print('err was {}'.format(x.stderr))
			print('out was {}'.format(x.stdout))
		print("job id {} started".format(self.jobid))

	def is_running(self):
		a = 'scontrol show job {} '.format(self.jobid)
		x = subprocess.run(shlex.split(a),shell=False,capture_output=True,text=True)
		state = [ s for s in x.stdout.strip().split() if 'JobState' in s]
		self.returncode = 0
		if len(state) > 0:
			s = state[0].split('=')[1]
			if s in ['COMPLETED','FAILED','TIMEOUT','CANCELLED']:
				self.returncode = 0 if s == 'COMPLETED' else 1
				return False
			else:
				return True
		else:
			return False

	def wait(self):
		while self.is_running():
			time.sleep(5)

if __name__ == "__main__":
	cmd = 'hostname && sleep 10'
	directory = './'
	config = configparser.ConfigParser()
	config['SLURM']={'time':'12:00:00',
		'nodes':1,
		'mpis_per_node':32,
		'cores_per_mpi':1,
		'name':'noname',
		'constraint':'amd',
		'memory':'4G',
		'preamble':"""module purge
		module load foss/2021a
		export PATH=/work3/cse/dlp/n2p2/bin:$PATH"""}

	with open('slurm.ini','w') as f:
		config.write(f)
	if os.path.isfile('slurm.ini'):
		t= configparser.ConfigParser()
		t.read('slurm.ini')
		s = t['SLURM']

		task = sbatch(nodes=s.get('nodes',1),mpis_per_node=s.get('mpis_per_node',32),
			constraint=s.get('constraint',None),jobfile=s.get('name','noname.slurm'),
			cmd = cmd,workdir=directory,time=s.get('time','00:10:00'),pre=s.get('preamble',None),
			account=s.get('account',None),reservation=s.get('reservation',None),
			memory=s.get('memory',None))
		task.run()
		task.wait()
