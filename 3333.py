import  subprocess

p = subprocess.Popen(['python', '%s' % self.cmd, '%d' % self.add_a, '%d' % self.add_b], stdout=subprocess.PIPE, encoding="utf-8", stderr=subprocess.PIPE, shell=False, universal_newlines=True)
