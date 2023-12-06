// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package auth

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

const validBase64CaData = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURhekNDQWxPZ0F3SUJBZ0lVS0R0SG5kbnlaSldQV21ONnBEYmY2aTZZYVlnd0RRWUpLb1pJaHZjTkFRRUwKQlFBd1JURUxNQWtHQTFVRUJoTUNRVlV4RXpBUkJnTlZCQWdNQ2xOdmJXVXRVM1JoZEdVeElUQWZCZ05WQkFvTQpHRWx1ZEdWeWJtVjBJRmRwWkdkcGRITWdVSFI1SUV4MFpEQWVGdzB5TXpFeE16QXdOVEEwTURsYUZ3MHlNekV5Ck16QXdOVEEwTURsYU1FVXhDekFKQmdOVkJBWVRBa0ZWTVJNd0VRWURWUVFJREFwVGIyMWxMVk4wWVhSbE1TRXcKSHdZRFZRUUtEQmhKYm5SbGNtNWxkQ0JYYVdSbmFYUnpJRkIwZVNCTWRHUXdnZ0VpTUEwR0NTcUdTSWIzRFFFQgpBUVVBQTRJQkR3QXdnZ0VLQW9JQkFRRFZkQ1FraGVHeU5LVnNib2xRMUFlK0xONmJ4eHNzQjMvcWNEWmdDclVlCkwxbEF6WHJYN1FJT2k4MUE5Nk5LUWQzOTZON3VRRE1jaWdDak1lS2pWNTRDQzNwejBXaVpmWVdHSzBiMXU3NHUKR1hhejdRa01DMXFyRWpSWjhoYml3M1orbGx1TGxlQXkzdkpNUmt3OEdqcHF4eVdrNFZNd2lzR1MzVXZBV0pqSgo3M0VRa0UyL09Ia05tbXEwTWlFcU5JVWdUZFNCR3dqSWh1UmtwVDlUQkhWNTNvcDduTjJzaHgzMUZWalIycEdsCkY0b05WOGl0RVNSVVJzOHRDaHJNWlcyQ0JLN1lSRzRKbVBKL2NYQ3grMW1aTmJId2JDOWdySVdRQis5bUNUZC8KYXV6VW85Y2hXdy9xNjROV1BOQnZsdnV1SFhNUVkyZkhkaU5pVG1XbHdLMHpBZ01CQUFHalV6QlJNQjBHQTFVZApEZ1FXQkJROFZEbkpkNkhlcXVINk1xKzNHV3QxWG8xVUtqQWZCZ05WSFNNRUdEQVdnQlE4VkRuSmQ2SGVxdUg2Ck1xKzNHV3QxWG8xVUtqQVBCZ05WSFJNQkFmOEVCVEFEQVFIL01BMEdDU3FHU0liM0RRRUJDd1VBQTRJQkFRQ2wKd0NDRSttUlp4eWNBK0pIODduN05naitSQjA1ak5WTlpCb2UzQUNZRmwrL3NFbERMc3UzRmR3ZjRHOW91L0x2bQpDVVRTaHNFQmQ4YkZpWlVKN0dLSmloTHdESjRYS2lBckNHS0p4d3doVEtZUDlzaDAyUXNYV0kwc1orN1c4bll5ClJLM2Q0NFpySWczQXJDaFBXcEdqUmtWVy9YWTdTamxzWUxRSFNFR2tMLzlSeW1UWVIycGQ5czhGZXFremFkbHIKSm1jRUlFZFNtbXNjcDNQNEk3MW43bHFXRTIyVDluUkFMczlKK1hkMm9IbkhLazRkcTU1QXpsU3A4ODM3aU9FTwp2L25RZzFNUjJPOE9jcHAyU1JsYmlsbVlmeGRJaytLZXNZSmVDd1NiaUxucmNJeHdtdWJBZ0E3TjVWN1pFa1paCnVFY2c3TGV6cUJMaUVUUm1Ga01OCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0="
const invalidBase64CaData = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURvRENDQW9pZ0F3SUJBZ0lVTDFKdUx0K0dRcWNuM0pDZGNiaUxibjBmSjhBd0RRWUpLb1pJaHZjTkFRRUwKQlFBd2FERUxNQWtHQTFVRUJoTUNWVk14RXpBUkJnTlZCQWdUQ2xkaGMyaHBibWQwYjI0eEVEQU9CZ05WQkFjVApCMU5sWVhSMGJHVXhEVEFMQmdOVkJBb1RCRlZ1YVhReERUQUxCZ05WQkFzVEJGUmxjM1F4RkRBU0JnTlZCQU1UCkMxVnVhWFJVWlhOMElFTkJNQjRYRFRJd01Ea3hOekUzTXpVd01Gb1hEVEkxTURreE5qRTNNelV3TUZvd2FERUwKTUFrR0ExVUVCaE1DVlZNeEV6QVJCZ05WQkFnVENsZGhjMmhwYm1kMGIyNHhFREFPQmdOVkJBY1RCMU5sWVhSMApiR1V4RFRBTEJnTlZCQW9UQkZWdWFYUXhEVEFMQmdOVkJBc1RCRlJsYzNReEZEQVNCZ05WQkFNVEMxVnVhWFJVClpYTjBJRU5CTUlJQklqQU5CZ2txaGtpRzl3MEJBUUVGQUFPQ0FROEFNSUlCQ2dLQ0FRRUF0ZzU5SGU2MDVlYjIKcThGYUpycHBoRVNPZnJiVEdIQlhXRk41Z0N1QUlZMmVNTUdKSnFwWERrUjNGWko2TFZaYXFkVm9rWmkzeVRIOQprWW5uTEhBRDJJKzd5M0FnczB0WWZucmx0MGhtWjNleVlRSGk0Y1d0Vkd3aVoycW0yQnZMbzJVMENkeXRSSjRRCjNlQVQyeVRrTnZ4Wm9XeUhHK09icjZ4UFByMjh2bWo3Q0txVnNLQ0FIVnlqdXlybXRJcHdkbWVpVTlFbTFTTUgKSVBLR0pJQ29NeGl4NXNDdHVqZmRSTWJTU2hIRFluUmdmMkx2enIxVk5mZkdaS01YekJaekkyZ3BJZm9YaGZVUwpkdmNlUTVoWXo4emdEY2hDOG1laEM3bU12Myt6Q3d6OWtGbmJpYnBvSVdGcStGbzYzeHNnc255dFlQTXY0cmltClgwSWRwZlA2VVFJREFRQUJvMEl3UURBT0JnTlZIUThCQWY4RUJBTUNBUVl3RHdZRFZSMFRBUUgvQkFVd0F3RUIKL3pBZEJnTlZIUTRFRmdRVWUzY0MvMllrTWRmRUZUbUliMW84M1U0VWgxY3dEUVlKS29aSWh2Y05BUUVMQlFBRApnZ0VCQUN2TTVURG9BM0FFRFlrcFlueWwwVlpmRDdKRVhWSEJ5WTEyeG9jUHM4TGJzNEtNS1NtUGVld0dIU25WCisrQVdFdG8vWlFjUnVVcm9SR2ZFRDRTU3kyT0tyNGh4M0J0UmNGRkZrdFg4U2Uwck5rSitaSHVoVFBWdWQ5L00KUXRBenl2UWVkcDBXQlcydDBvWkhDcVNOWmMvSWFYWGNxeTdocHpLOHBLZTNOUXYyUkdHVkEybWRDR1oxUE5rMgpFTXhMVnhoUURkbTNKRWJ4SEJPNCtVWm45MHVDd1BGc25rVFFmNm53WTErMjNMc3lheFkxWXFkeXZHTzhjdEc0CnozUmRTbTVJM25XaTNERFd3TnhuZ1lpMCtBL01VQ0FBYjBOejluSXI3dzB5UlJpWHJha1hUYjlaOC9GWE1JdHEKdG5wckJzK3hhYzhBVWxzcEw3cCtUWmRUMFdNPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg=="
const validBase64Certificate = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURSVENDQWkyZ0F3SUJBZ0lVSkVIL2x2ZnZZeE5LRkJJME05SVE1VXhqQUh3d0RRWUpLb1pJaHZjTkFRRUwKQlFBd1JURUxNQWtHQTFVRUJoTUNRVlV4RXpBUkJnTlZCQWdNQ2xOdmJXVXRVM1JoZEdVeElUQWZCZ05WQkFvTQpHRWx1ZEdWeWJtVjBJRmRwWkdkcGRITWdVSFI1SUV4MFpEQWVGdzB5TXpFeE16QXdPRE00TURSYUZ3MHlOakF5Ck1EY3dPRE00TURSYU1CUXhFakFRQmdOVkJBTU1DV3h2WTJGc2FHOXpkRENDQVNJd0RRWUpLb1pJaHZjTkFRRUIKQlFBRGdnRVBBRENDQVFvQ2dnRUJBT3c4cDNUY3RxNVpmV2ZST29lMmlPVG9hT3dWclpRbkgwc0Nla0NRMlVzSAppR3k4c054Q28rWnZrZmZuOUJ6ZlJWNVJjUk53R1l6VEVVTm1NTnhUdjdldlFBVll0TTJRY0lHWGluSDB1aytlClAvTFo5bkxtUWdjdWxzZnhHWlh2L1FHSG9LOVJ6bjRabUdSZ3Z3QzdSOVVQT0k1YWxyRk5rTmJrZHZxTm5kVnoKV3FXcEFhSXNTZkNuM0E0ZlBLemc1SFpTYmZNSXc0YjJsa25Pdk1XTXY5Lzg0Yitjc2RkSFcvTXZNcnVXQUZhLwoxV0lKVlozTlljTm95Wk5CcDh2VExZNEJ5VVZMV2lCRXZhWTh2OWo0dkVleGlXQ0s5U0pVcndoSlZ5OWI5cmFhClRjbXl6RUJLb3NTUWgxemJDOHBuQStwUWFERXBKYmdncEdzZjN5aFJuWWtDQXdFQUFhTmVNRnd3R2dZRFZSMFIKQkJNd0VZSUpiRzlqWVd4b2IzTjBod1IvQUFBQk1CMEdBMVVkRGdRV0JCU09hS1Q5MWR3WUVXWXUvMjI3QlljcQovSTBZaFRBZkJnTlZIU01FR0RBV2dCUThWRG5KZDZIZXF1SDZNcSszR1d0MVhvMVVLakFOQmdrcWhraUc5dzBCCkFRc0ZBQU9DQVFFQUlIT2tCcnpXLzdjbkx1L0JPQ05oY0t2YlBVNGFXRzZhTkhWc2dwclpjVk1taFhpNFNzMzUKOE9kalRjTGVJZXpuWVJFNWZQdnpYcHlMSXpwU3FNTzkyNXRTVHV2ODl1d3BHUnlWeUlWcVJBek01clQvbGc5OQp0UjAza1QzRVlKeVM3SldWc3VDMkgzdUhEQzE1RXBmZ2lJS0NWckFDUXoxS1BFZVZyTnl1Z0NlWFNEWXhoRjRTCnhwRURzQ1oyQWhKNjRSQTZ6bmg2MUxKU1M3bXVQd05uYmYzTzVIekx3MU94ajFEcFVkdU8rNVEwUzUzd241Z0kKcmlIR29hVnFNUFhEM2JJTm5WSjNxelFMYm5NYVZRcUpGYUpsOGFzUnVvem5PN2xOWDdmY1UvN0FyUVAyaDhWVQp6N0ZRYzhoUXlVd21UcjlPanRGM2pOZC9nUFFYcTFlL0FBPT0KLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo="
const invalidBase64Certificate = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURPVENDQWlHZ0F3SUJBZ0lVZnFUak95anpOVWdqQVZ1Qm51VFVhNkhMWHkwd0RRWUpLb1pJaHZjTkFRRUwKQlFBd05ERXlNREFHQTFVRUF4TXBSV3hoYzNScFl5QkRaWEowYVdacFkyRjBaU0JVYjI5c0lFRjFkRzluWlc1bApjbUYwWldRZ1EwRXdIaGNOTWpNeE1URTRNRGMxTmpFMFdoY05Nall4TVRFM01EYzFOakUwV2pBVU1SSXdFQVlEClZRUURFd2xzYjJOaGJHaHZjM1F3Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLQW9JQkFRQ0QKQ3NrTFpWMDJjWXpYZ21oRjhPTnlsVEZaTkdiRmVBSG0wenQyVHJESElNSUg4bk8yWm5xYkU1TEdrZHJ6VG1SUwpKTEVKTUNnV2pDQWxMMktLdzRZNW01dnc3YlptWUU1VDhEeXZDZGV3L1hSblZLdW1HUFhmd0c5SUExSVhqbHUrCk9VaVR1emVmZER6d3kvTmQrdVlRbkFyMkJ3aFQ4b3JRZnlTRlNGK2ZOR1llcWh4TjcwaWVQS3pUUU1mL0pwZ1MKYUJvV2F0WTdKTFRMWC9YTW11dWtyaytDTUk2QU1SWWFlNUE4UmNWZnBub3c5c24ycThvZWs5cHNiTHJsWURneAo0by9YMm5YYkhPSGtrTExzWHdSRzNLdWIzZ2RMN1BQcUg3RVhhSW9SOUZ0ZHM3YzF1NWhidVpXRXlkUWF2VXVsCm9jMmlGWDBWODZoaVlhUDN1MktKQWdNQkFBR2pZekJoTUIwR0ExVWREZ1FXQkJTUlZMaTN0c0lMZU5Wd1NjM0gKRHR0dnJHWVIxREFmQmdOVkhTTUVHREFXZ0JSUi8xL2Y1dnlWcEk2V1ZYWU1PQURwRnpGYmZUQVVCZ05WSFJFRQpEVEFMZ2dsc2IyTmhiR2h2YzNRd0NRWURWUjBUQkFJd0FEQU5CZ2txaGtpRzl3MEJBUXNGQUFPQ0FRRUFhNnNDCjZ5ME1meFdrRjBEUjhZVjlSdmRiZ0svczFhenA4ZHcydmJML0plTExaNFJ0ekFWTW8zNHlxajZmbWJJY0YyT1gKTCtGUENDYXZvM0oxUDZadkpaZUtNdmQ4NEtIUzJQVVYvREYxRXVWQkhJME85RWZmM1BDS0VTVmZUS29QbnlsSQpjKzY5L0N6b2NKTDFINlppQ1RHMWVQT21aZlNYNUdnN3g5cmM5SnhXZmlWdUhjcFJadTM0aUhpT2xCMTk0Y2FSCkwxL1R3ZGJ4cmNveUFNUGFyQURFL212dDRqTXQ4ZFk3anowR2E0Nk55RkUySlZrenFLMlpGbEZ4UTYxdDZvWWMKZWk3NzdxTFlwUVB1bERHVkVvM2d0d0h2TmpYQ0NHcTJNQzZHM0JjMGxlVlFQRkF1VE9hdkI4c1FRNnR4eldKOAorN0JFL1FaZmtySUZPOGFKZ0E9PQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg=="
const validBase64Key = "LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCk1JSUV2UUlCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktjd2dnU2pBZ0VBQW9JQkFRRHNQS2QwM0xhdVdYMW4KMFRxSHRvams2R2pzRmEyVUp4OUxBbnBBa05sTEI0aHN2TERjUXFQbWI1SDM1L1FjMzBWZVVYRVRjQm1NMHhGRApaakRjVTcrM3IwQUZXTFROa0hDQmw0cHg5THBQbmoveTJmWnk1a0lITHBiSDhSbVY3LzBCaDZDdlVjNStHWmhrCllMOEF1MGZWRHppT1dwYXhUWkRXNUhiNmpaM1ZjMXFscVFHaUxFbndwOXdPSHp5czRPUjJVbTN6Q01PRzlwWkoKenJ6RmpML2YvT0cvbkxIWFIxdnpMeks3bGdCV3Y5VmlDVldkeldIRGFNbVRRYWZMMHkyT0FjbEZTMW9nUkwybQpQTC9ZK0x4SHNZbGdpdlVpVks4SVNWY3ZXL2EybWszSnNzeEFTcUxFa0lkYzJ3dktad1BxVUdneEtTVzRJS1JyCkg5OG9VWjJKQWdNQkFBRUNnZ0VBQ21nekk5aXYwdzgxOFRlRVRiTit4Ui9RZEhzUmdDWDNrTk5CZmJ2cjJLeHkKcUFDcGhYQ0puaWFTM29JWlROYlFyR0tuMmJoOTVhaHNPYVVFT01hWE92RTFYNjdzV3lSMmNsMFIwQ3FjOVJJMgpoTDB4cUZib3VINkd6YlRiUU5IWFdtU0dVWWJuOHdIdlp0ZWt2blJocWhzUExhVkR2M2lZYllFWDFUcWRKZ2ltCjJtcXBmbUh4VmloNWlhVGxjZ3ZUeUNLN2ZTZzdRalFkK3d1Wk9IZzZveExpU3J4WFRZTmk3UUlTK25oNWk0djcKMWd4cEFqa2JMeFZtNE9TZVFaYzVaREU0YU1xUzh6Sjl2VXNRRGduUHorbStSOGhWbFpEK1FIVmV4TmE2K0ZpSQpHbEdBanEvMXV4YU5BYkUvUENudzc5T004aDVkaFllc0ZTcWVGc2Qwd1FLQmdRRC9GOXVpQW9rdGtLcm91RWxhClh1ci9RTWtyQU5KWHEzSnJCZ0pBVExUNVVsUmlkc21EeVRJMXRUbXFOVVZTSFA3NG5RYVUvRDhNV3Z0YXVBMzIKU3pEOEJzTmNGSlB2RjBoWmRRL21PV1d0elU4aFRQR3Zac201ek44YkdYdWxLcUFwSG9kL0pxUGFUcm5kRk9JUgpuWkc3SnRlZlVLN3d6cEY2djNCN1ZpcE95UUtCZ1FEdEU2THVVbmVRMVZ2akcvd0dyN0hLVFJ3a2dsQjBDd0tFCmtCU3ZHS2JvdzVidUxXR0pFS1g1YXZoUkJsQlRFa2dqVzlhREs5a2N5V1FkY1V3czBzRWVXWTdjNmJHVDZPME8Kb2xBWlB1KzlsSmVvV1QwTHRRcDRRYlgxNG84UERJbGt0bUJrV1JxbDYxVjh5SCtzTlpMWnlzYnZEbWNicng2eQptZzR0V281NHdRS0JnRDFMK2xiZnpSN0oySWU3WU1UNmNmV01GOHJoazZuRlpPVWF1SWZDNXVuU1FyeTJWbzM0ClZyeFpJOVltbnRXd2FnUkxsejFOcjhqRVJBbjBtRVpLb3lhc2FWMURCSms3T3dOa0FjSU1vTVkydzRENUFFcHAKcEtlazl5ZUg1Qkk0UCt3aGplV2IzMXVoOXorTXVSWUtpdTR4MHpaUktQaHNhc0RZSjZzN2RVY2hBb0dCQUpoRwp4NTJLak5BVHEweXFHZXgxaDU0b2YwNFlBZk0zYXl1WW5DQ0hsaFhtSTVqaXYwWlowakh0ZW9nWXBSbG1vYjFNCmJQR2VCWHVQQStaQmNxdEx4ODFsdXZTOGlsbzEvNllwclljNXZLV3B2dXZjUGZDNkhYcDJ3cGlvS0RtRFZQREMKa0JHRWhqQlNnM3QrRVR1Y3diRndwT3pCOUlwOHBod1VCYzB0NEZ0QkFvR0FHY1VHQk1OWG5UZnk5OEdqTytEeAp2eTRjTXRhdXE2RzJjUEs3Q1FXcmgrbjZVcUgrZ1Rjd2NxOUJYa0NIVTgxK0NUOUFQRTVzaUhEZlZsaHJ2bm5LCjJXbDh3R1o3ZHVlK3lpVnFJbFZ3cUswcmNtRmdOTldwby8zc096MnFSb2NWazQ3T3Q1bjhVZVc0MUdJV2swQVcKNTVuMVFpZU92WEFOMnBQb3Rxd0VkNTQ9Ci0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS0K"
const invalidBase64Key = "LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFb3dJQkFBS0NBUUVBZ3dySkMyVmRObkdNMTRKb1JmRGpjcFV4V1RSbXhYZ0I1dE03ZGs2d3h5RENCL0p6CnRtWjZteE9TeHBIYTgwNWtVaVN4Q1RBb0Zvd2dKUzlpaXNPR09adWI4TzIyWm1CT1UvQThyd25Yc1AxMFoxU3IKcGhqMTM4QnZTQU5TRjQ1YnZqbElrN3MzbjNRODhNdnpYZnJtRUp3SzlnY0lVL0tLMEg4a2hVaGZuelJtSHFvYwpUZTlJbmp5czAwREgveWFZRW1nYUZtcldPeVMweTEvMXpKcnJwSzVQZ2pDT2dERVdHbnVRUEVYRlg2WjZNUGJKCjlxdktIcFBhYkd5NjVXQTRNZUtQMTlwMTJ4emg1SkN5N0Y4RVJ0eXJtOTRIUyt6ejZoK3hGMmlLRWZSYlhiTzMKTmJ1WVc3bVZoTW5VR3IxTHBhSE5vaFY5RmZPb1ltR2o5N3RpaVFJREFRQUJBb0lCQUg1MEIvSFJUUlBlbTNUVAp5Ti9GUnhjcFZVZXB3NHJHOWI3VEU4eGt2ejVKSkRRYkNRSjQvZE5zSGZVMGhyN0haUlBIaUhjL1cwLzJ4SVpkCnBaQVdnZzVSVlRnM2pBNWEzUHN2RnNBcWxWT2NJWm9kSU03VncxNjZDaWpKMjR3VHVnQmtzdDZzaVU1OEV0cWoKVlNQWm0rMW5SMFNISU1neGd6Y1RtaUJyNktwdHVtWXhlLzZPUzRBNUZhYzU0aFZRYUhyM0xwRWkyRW9mWnBPSApFUEJwNmZNcU1HYzZ3NVliSTZuKzNtZDB4akx0dVp0TFB6NkVnV0MwRG04RWduc0pIaWYzaGQ0QUFmbEFmRXRMCmhzbWEwNDFGdVA5VHkvbGhIbkR4R1ZEL1EySnA5eG5EZitHa0RESmxEUG9nbDBlelh0aGcwd1dyeVdMVFcveFAKTWZHclBLRUNnWUVBdmtFQTUyc1FjM2FzNkl3OEY0Uzlkdm5taEJxUVhsZzdxN0NaZzMvK3djTXFIK05NN2d3OQpJTDhocWdIbTBrUjBXcEJITFdld2hhNlNidlM5RFEwemluT1NKam54cGVsNnRvRU9zaEdObjhlQnpjSGozYWpnCkNRaGJiMTRMSmxoZFVrRTJoekg1NllQazZTcmtDVE94MGdSV2ZOczVTRjZWWjZpZUU2UXFkUDBDZ1lFQXNGT04Kd0srUXplUHpVb05GRUFOTFBYZGI1TERTdHZ0V091UDV2NHRST2dMNzgwQ0N2WERYV1RjSithSHR2d3IwckhHQgovMFhKb2NNU1ZlbDBNMTBXR0N6MlBjTnJEUWZFQ1ZKMVorTHlvU3o5aHk1R3hNeERIc1NuMnZadUFrU2JhTW9nCmpEcjBZVXlGRWFaMFBmbnIrdHVMb2JXWXNybnNaN3VqSzgzdVAzMENnWUFaM09KUGt6bFlTT0MyZUNIUEhLZFkKM2gzZEJYTnNyOWZrdmd3UVdUejdnQmxnM2xoZDMrSUxhcEFiK1VnMGUwUEo3K1VOSWhSWUIrUTJHeVQ3K2podwpjTWVFVk1vcHdMU0N6TVovcEcwNU5Eak1ETGg2TUhQTUpvdXZkdnhUQ1I0ZWlXanROZmtBS29MYXc0N2VSVjI5CjdBTUoxQTlVYkM4cE9UM0w4N3dselFLQmdGTjdxOURBRHdvOFUvY01LY1cwR1lxSG1aMkVUcS9OL2Z5eDlZeEgKOVBSSGQyeXJiWHN1RXZldGhHNkp5VnU3WHk3S0t5ZG1ybG1GVjRnUG1URzhiL3FRUnlIbEJTbE5OUGJJOE1kMwovekpxYjdyeUlSV0tOSGs3Mk5GbC9aM2JSODFzYmM2WEZ4OStNRDYwRmJOR1FnRXFzMGlrQnlFUHdDczQvcjk5CnA4Q3RBb0dCQUo3MkZRRkVaeEZOZE5PQ3QxS1V0VTZSUDBuSCsxM0xpWWRrZ2hjQi94VGpqZVpNekZpdFh6VlEKd2QvRjlFczYzcEtIWjBSQzY4MndZWTk5OFAzb3NVWXk2V3hIcENUK3N3ZUFQRUZCT1FsNE9FTG5HNWVKbkE0UApPT2FabFVKWVpxNmsrc09SVE1GdG5nU0ZQOWNLSXc1WHZJa1daMzhFdkhsYnhUa1lDcFJqCi0tLS0tRU5EIFJTQSBQUklWQVRFIEtFWS0tLS0tCg=="

// test if the input is valid
func Test_NewTLSConfig(t *testing.T) {
	tests := map[string]struct {
		cfg    *TLS
		cfgErr string
	}{
		"emptyConfig": {
			cfg: &TLS{},
		},
		"caData_good": {
			cfg: &TLS{
				Enabled: true,
				CaData:  validBase64CaData,
			},
		},
		"caData_badBase64": {
			cfg:    &TLS{Enabled: true, CaData: "this isn't base64"},
			cfgErr: "illegal base64 data at input byte",
		},
		"caData_badPEM": {
			cfg:    &TLS{Enabled: true, CaData: "dGhpcyBpc24ndCBhIFBFTSBjZXJ0"},
			cfgErr: "unable to parse certs as PEM",
		},
		"clientCert_badbase64cert": {
			cfg: &TLS{
				Enabled:  true,
				CertData: "this ain't base64",
				KeyData:  validBase64Key,
			},
			cfgErr: "illegal base64 data at input byte",
		},
		"clientCert_badbase64key": {
			cfg: &TLS{
				Enabled:  true,
				CertData: validBase64Certificate,
				KeyData:  "this ain't base64",
			},
			cfgErr: "illegal base64 data at input byte",
		},
		"clientCert_missingprivatekey": {
			cfg: &TLS{
				Enabled:  true,
				CertData: validBase64Certificate,
				KeyData:  "",
			},
			cfgErr: "unable to config TLS: cert or key is missing",
		},
		"clientCert_duplicate_cert": {
			cfg: &TLS{
				Enabled:  true,
				CertData: validBase64Certificate,
				CertFile: "/a/b/c",
			},
			cfgErr: "only one of certData or certFile properties should be specified",
		},
		"clientCert_duplicate_key": {
			cfg: &TLS{
				Enabled: true,
				KeyData: validBase64Key,
				KeyFile: "/a/b/c",
			},
			cfgErr: "only one of keyData or keyFile properties should be specified",
		},
		"clientCert_duplicate_ca": {
			cfg: &TLS{
				Enabled: true,
				CaData:  validBase64CaData,
				CaFile:  "/a/b/c",
			},
			cfgErr: "only one of caData or caFile properties should be specified",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			_, err := NewTLSConfig(tc.cfg)
			if tc.cfgErr != "" {
				assert.ErrorContains(t, err, tc.cfgErr)
			} else {
				assert.NoErr(t, err)
			}

			ctrl.Finish()
		})
	}
}

func Test_ConnectToTLSServerWithCA(t *testing.T) {
	// setup server
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello World")
	})
	ts := httptest.NewUnstartedServer(h)
	certBytes, err := os.ReadFile("./testdata/localhost.crt")
	if err != nil {
		panic(fmt.Errorf("unable to decode certificate %w", err))
	}
	keyBytes, err := os.ReadFile("./testdata/localhost.key")
	if err != nil {
		panic(fmt.Errorf("unable to decode key %w", err))
	}
	cert, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		panic(fmt.Errorf("unable to load certificate %w", err))
	}
	ts.TLS = &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	ts.StartTLS()

	tests := map[string]struct {
		cfg           *TLS
		connectionErr string
	}{
		"caData_good": {
			cfg: &TLS{
				Enabled: true,
				CaData:  validBase64CaData,
			},
		},
		"caData_signedByWrongCA": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaData:                 invalidBase64CaData,
			},
			connectionErr: "x509: certificate signed by unknown authority",
		},
		"caData_signedByWrongCAButNotEnableHostVerification": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: false,
				CaData:                 invalidBase64CaData,
			},
		},
		"caFile_good": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaFile:                 "testdata/ca.crt",
			},
		},
		"caFile_signedByWrongCA": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaFile:                 "testdata/invalid_ca.crt",
			},
			connectionErr: "x509: certificate signed by unknown authority",
		},
		"caFile_signedByWrongCANotEnableHostVerification": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: false,
				CaFile:                 "testdata/invalid_ca.crt",
			},
		},
		"certData_good": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaData:                 validBase64Certificate,
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			tlsConfig, err := NewTLSConfig(tc.cfg)
			if err != nil {
				panic(err)
			}
			cl := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
			resp, err := cl.Get(ts.URL)
			if tc.connectionErr != "" {
				assert.ErrorContains(t, err, tc.connectionErr)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, 200, resp.StatusCode)
			}

			ctrl.Finish()
		})
	}
}

func Test_ConnectToTLSServerWithClientCertificate(t *testing.T) {
	// setup server
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Hello World")
	})
	ts := httptest.NewUnstartedServer(h)
	certBytes, err := os.ReadFile("./testdata/localhost.crt")
	if err != nil {
		panic(fmt.Errorf("unable to decode certificate %w", err))
	}
	keyBytes, err := os.ReadFile("./testdata/localhost.key")
	if err != nil {
		panic(fmt.Errorf("unable to decode key %w", err))
	}
	cert, err := tls.X509KeyPair(certBytes, keyBytes)
	if err != nil {
		panic(fmt.Errorf("unable to load certificate %w", err))
	}
	caBytes, _ := os.ReadFile("testdata/ca.crt")
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caBytes)
	ts.TLS = &tls.Config{
		ClientCAs:    caCertPool,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	ts.StartTLS()

	tests := map[string]struct {
		cfg           *TLS
		connectionErr string
	}{
		"clientData_good": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaData:                 validBase64CaData,
				CertData:               validBase64Certificate,
				KeyData:                validBase64Key,
			},
		},
		"clientData_certNotProvided": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaData:                 validBase64CaData,
			},
			connectionErr: "certificate required",
		},
		"clientData_certInvalid": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaData:                 validBase64CaData,
				CertData:               invalidBase64Certificate,
				KeyData:                invalidBase64Key,
			},
			connectionErr: "certificate required",
		},
		"certFile_good": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaData:                 validBase64CaData,
				CertFile:               "testdata/localhost.crt",
				KeyFile:                "testdata/localhost.key",
			},
		},
		"clientFile_certInvalid": {
			cfg: &TLS{
				Enabled:                true,
				EnableHostVerification: true,
				CaData:                 validBase64CaData,
				CertFile:               "testdata/invalid_localhost.crt",
				KeyFile:                "testdata/invalid_localhost.key",
			},
			connectionErr: "certificate required",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			tlsConfig, err := NewTLSConfig(tc.cfg)
			if err != nil {
				panic(err)
			}
			cl := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
			resp, err := cl.Get(ts.URL)
			if tc.connectionErr != "" {
				assert.ErrorContains(t, err, tc.connectionErr)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, 200, resp.StatusCode)
			}

			ctrl.Finish()
		})
	}
}
