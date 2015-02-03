#!/usr/bin/ruby

count = ARGV[0].to_i

amount_dist = [
10,10,
17,17,17,
25,25,25,25,25,
37,37,37,37,37,37,37,
45,45,45,45,45,
66,66,66,66,
82,82,82,82,
150,150,150,
220,220,
300,300,
500,
1000,
2000
]

time_dist = [
0,0,0,
1,1,1,1,
2,2,2,2,2,2,2,
3,3,3,3,3,
4,4,4,
5,5,
6,
7,
8,
9,
10,
11,
12,
13,
14,
15,
16,16,
17,17,17,
18,18,
19,
20,
21,21,
22,22,22,
23
]

vendors = ['grocery', 'restaurant', 'drug store', 'super market', 'electronic store', 'clothing store', 'jewellery store', 
'air fare', 'hotel', 'car rental']

vendor_dist = [
0,0,0,0,0,0,0,0,0,
1,1,1,
2,2,2,2,2,2,
3,3,3,3,
4,4,
5,5,5,
7,7,7,
8,8,
9,9
]


vendor_amount_dist = {
'grocery' => [
10,10,
20,20,20,20,
30,30,30,30,30,30,30,
50,50,50,50,50,50,50,50,50,
70,70,70,70,
100,100,
150
],

'restaurant' => [
10,10,
20,20,20,20,20,
27,27,
35,
50
],

'drug store' => [
12,12,
23,23,23,23,23,
37,37,37,
45,45,
60
],

'super market' => [
25,25,
38,38,38,
49,49,49,49,49,49,
68,68,68,
112,112,
185,
250
],

'electronic store' => [
60,60,
90,90,
120,120,120,120,
190,190,190,190,190,
250,250,250,
300,300,
500
],

'clothing store' => [
30,30,
50,50,50,50,
70,70,70,
90,90,
150,
200
],

'jewellery store' => [
100,
170,170,
260,260,260,
310,310,
400
],

'air fare' => [
110,110,
180,180,180,
310,310,310,310,310,
520,520,
600
],

'hotel' => [
110,110,110,
230,230,230,230,
300,
400
],

'car rental' => [
60,60,
110,110,110,110,
150,150,
200
]

}

key = ['0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O',
'P','Q','R','S','T','U','V','W','X','Y','Z']

def gen_id(key)
	id = ''
	1.upto 8 do #!/usr/bin/ruby

require '../lib/util.rb'      

userCount = ARGV[0].to_i

		id << key[rand(key.length)]
	end
	return id
end

def sample(dist, mult, floor, percent)
	b = rand(dist.length)
	val = dist[b]
    val = val * mult
	percent = rand(percent)
	percent = percent < floor ? floor : percent

    dev = (val * percent) / 100
	if ((rand(100) % 2) == 0)
		val = val + dev
	else 
		val = val - dev
	end
	val = val < 0 ? 0 : val
	val
end

1.upto count do 
	id = gen_id(key)
	time = sample(time_dist, 60, 2, 8)
	time = time > 1440 ? 1440 : time
	v = vendor_dist[rand(vendor_dist.length)]
	vendor = vendors[v]
	am = sample(vendor_amount_dist[vendor], 100, 4, 12)
	puts "#{id}[]#{time}[]#{am/100}.#{am%100}[]#{vendor}"
end


